/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.CollectorResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the one place fused mode rebuilds a response.
 *
 * <p>What is worth pinning here is not the two overrides — those are obvious from the call — but everything the rebuild
 * copies by hand and no caller looks at. A field dropped there would silently change an answer the search already got
 * right, and only on requests that reported something, so the assertions below name every one of them and the last test
 * fails when core adds one.
 */
public class FusedResponseRebuilderTests extends OpenSearchTestCase {

    private static final String SHARD_KEY = "[node][index][0]";

    /** The cheap path: a request that collected nothing and did not time out gets its own response straight back. */
    /**
     * The 4-arg overload exists for the one field that cannot be corrected in place — {@code SearchHits.maxScore} is
     * {@code final} — so a {@code null} hits override has to behave exactly like the 3-arg form, including its
     * nothing-changed short circuit.
     */
    public void testRebuild_withHitsOverride_whenNothingChanges_thenTheSameResponseIsReturned() {
        SearchResponse completed = responseWithProfile(Map.of(SHARD_KEY, shardResult(1)));

        assertSame(
            "a null hits override plus an unchanged timeout flag is a no-op",
            completed,
            FusedResponseRebuilder.rebuild(completed, null, false, null)
        );
    }

    /** And when hits ARE supplied they replace the response's own, with every other section carried across. */
    public void testRebuild_withHitsOverride_thenTheHitsAreSubstitutedAndTheRestPreserved() {
        SearchResponse response = responseWithProfile(Map.of(SHARD_KEY, shardResult(1)));
        SearchHits replacement = new SearchHits(
            new SearchHit[0],
            new org.apache.lucene.search.TotalHits(7, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            0.0f
        );

        SearchResponse rebuilt = FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), replacement);

        assertNotSame(response, rebuilt);
        assertSame("the supplied hits are the ones carried", replacement, rebuilt.getInternalResponse().hits());
        assertEquals(7, rebuilt.getInternalResponse().hits().getTotalHits().value());
        assertEquals("the point in time id survives the substitution", response.pointInTimeId(), rebuilt.pointInTimeId());
        assertEquals(response.getTotalShards(), rebuilt.getTotalShards());
    }

    public void testRebuild_whenNeitherOverrideChangesAnything_thenTheSameResponseIsReturned() {
        SearchResponse timedOut = responseWithEverything("scroll-id", null);
        assertSame(
            "no section to substitute and timed_out already set: nothing to rebuild",
            timedOut,
            FusedResponseRebuilder.rebuild(timedOut, null, true)
        );

        SearchResponse completed = responseWithProfile(Map.of(SHARD_KEY, shardResult(1)));
        assertSame(
            "no section to substitute and no timeout to report: nothing to rebuild",
            completed,
            FusedResponseRebuilder.rebuild(completed, null, false)
        );
    }

    /**
     * Every section outside the profile, through the widest constructors, with a distinct non-default value each — so that
     * dropping any single one from either argument list fails here.
     */
    public void testRebuild_whenTheProfileSectionIsSubstituted_thenEverySectionOutsideItIsPreserved() {
        SearchResponse response = responseWithEverything("scroll-id", null);

        SearchResponse rebuilt = FusedResponseRebuilder.rebuild(
            response,
            new SearchProfileShardResults(Map.of(SHARD_KEY + "[fused:hybrid_0.leg_0]", shardResult(2))),
            true
        );

        assertEquals(
            "the substituted section is the one the response carries",
            Set.of(SHARD_KEY + "[fused:hybrid_0.leg_0]"),
            rebuilt.getProfileResults().keySet()
        );

        // SearchResponseSections, all of it but the profile results the rebuild exists to replace
        assertSame("hits are the answer and must survive verbatim", response.getHits(), rebuilt.getHits());
        assertEquals(42L, rebuilt.getHits().getTotalHits().value());
        assertEquals(1.5f, rebuilt.getHits().getMaxScore(), 0.0f);
        assertSame("aggregations are handed over, not rebuilt", response.getAggregations(), rebuilt.getAggregations());
        assertSame("a suggest section belongs to the search, not to the profile", response.getSuggest(), rebuilt.getSuggest());
        assertTrue(rebuilt.isTimedOut());
        assertEquals(Boolean.TRUE, rebuilt.isTerminatedEarly());
        assertEquals(3, rebuilt.getNumReducePhases());
        assertEquals(
            "ext sections are what a pipeline answers with",
            response.getInternalResponse().getSearchExtBuilders(),
            rebuilt.getInternalResponse().getSearchExtBuilders()
        );
        assertEquals(
            "and so is the processor execution detail a verbose pipeline request asked for",
            response.getInternalResponse().getProcessorResult(),
            rebuilt.getInternalResponse().getProcessorResult()
        );

        // SearchResponse's own fields
        assertEquals("scroll-id", rebuilt.getScrollId());
        assertEquals(5, rebuilt.getTotalShards());
        assertEquals(4, rebuilt.getSuccessfulShards());
        assertEquals(1, rebuilt.getSkippedShards());
        assertEquals("took is the user's latency, not the rebuild's", 17L, rebuilt.getTook().millis());
        assertEquals("phase_took is the per-phase half of the same accounting", Map.of("query", 11L), phaseTookMap(rebuilt));
        assertEquals("a partial answer stays partial", 1, rebuilt.getFailedShards());
        assertSame(response.getShardFailures()[0], rebuilt.getShardFailures()[0]);
        assertSame(SearchResponse.Clusters.EMPTY, rebuilt.getClusters());
    }

    /**
     * {@code pointInTimeId} shares the assertion above's fixture but not its response: core asserts a response carries at
     * most one of {@code scrollId} and {@code pointInTimeId}, so the two can only be pinned one per response.
     */
    public void testRebuild_whenRebuilt_thenThePointInTimeIdIsPreserved() {
        SearchResponse rebuilt = FusedResponseRebuilder.rebuild(
            responseWithEverything(null, "pit-id"),
            new SearchProfileShardResults(Map.of(SHARD_KEY, shardResult(1))),
            true
        );

        assertEquals("pit-id", rebuilt.pointInTimeId());
        assertNull("and the scroll id it excludes stays absent", rebuilt.getScrollId());
    }

    /**
     * A rebuild driven by {@code timed_out} alone substitutes no profile section, so it has to hand back the one the
     * response already had — the profile section is {@code protected} in core, so it is reconstructed from
     * {@code profile()} rather than passed through, and this is what pins that round trip.
     */
    public void testRebuild_whenOnlyTheTimeoutFlagChanges_thenTheResponseKeepsItsOwnProfileSection() {
        SearchResponse response = responseWithProfile(Map.of(SHARD_KEY, shardResult(2)));

        SearchResponse rebuilt = FusedResponseRebuilder.rebuild(response, null, true);

        assertTrue("the flag the rebuild exists for", rebuilt.isTimedOut());
        assertEquals(
            "the response's own profile section is not collateral damage",
            Set.of(SHARD_KEY),
            rebuilt.getProfileResults().keySet()
        );
        assertSame(
            "and it is the same entries, not a copy of them",
            response.getProfileResults().get(SHARD_KEY),
            rebuilt.getProfileResults().get(SHARD_KEY)
        );
    }

    /**
     * The reconstruction above must not turn "this response carries no profile section" into an empty {@code profile}
     * block, which is what a section holding no entries renders as. Asserted on the rendering because that is the only
     * place the difference is observable: {@code profile()} answers with an empty map either way.
     */
    public void testRebuild_whenTheResponseHasNoProfileSection_thenNoneIsInvented() throws IOException {
        SearchResponse response = responseWithProfile(null);

        SearchResponse rebuilt = FusedResponseRebuilder.rebuild(response, null, true);

        assertTrue(rebuilt.isTimedOut());
        assertTrue("nothing to report", rebuilt.getProfileResults().isEmpty());
        assertFalse("an unprofiled response must not grow a profile block", render(rebuilt).contains("\"profile\""));
        assertTrue(
            "while a profiled one keeps its own",
            render(FusedResponseRebuilder.rebuild(responseWithProfile(Map.of(SHARD_KEY, shardResult(1))), null, true)).contains(
                "\"profile\""
            )
        );
    }

    /**
     * The rebuild is only complete while it calls the widest constructor core offers, and core widens a response by
     * <i>adding</i> a constructor rather than changing one — the 7 &rarr; 8 &rarr; 9 and 8 &rarr; 9 &rarr; 10 ladders these
     * two classes already carry are the evidence. So a new field arrives as a wider constructor, not as a compile error in
     * {@link FusedResponseRebuilder}, and nothing above would fail. This is the assertion that does fail, and it names the
     * fix.
     */
    public void testRebuild_whenCoreWidensAResponse_thenTheRebuildIsToldToCarryTheNewField() {
        assertEquals(
            "core widened InternalSearchResponse: FusedResponseRebuilder#rebuild must pass the new field(s) too",
            9,
            widestPublicConstructorArity(InternalSearchResponse.class)
        );
        assertEquals(
            "core widened SearchResponse: FusedResponseRebuilder#rebuild must pass the new field(s) too",
            10,
            widestPublicConstructorArity(SearchResponse.class)
        );
    }

    private static int widestPublicConstructorArity(final Class<?> type) {
        return Arrays.stream(type.getConstructors()).mapToInt(Constructor::getParameterCount).max().orElse(0);
    }

    private static Map<String, Long> phaseTookMap(final SearchResponse response) {
        return Objects.isNull(response.getPhaseTook()) ? null : response.getPhaseTook().getPhaseTookMap();
    }

    private static String render(final SearchResponse response) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.toString();
    }

    /**
     * A response with a distinct non-default value in every field the rebuild has to carry, through the widest constructor
     * each class offers. Only one of {@code scrollId} / {@code pointInTimeId} may be set, which core asserts.
     */
    private static SearchResponse responseWithEverything(final String scrollId, final String pointInTimeId) {
        SearchHits hits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(42L, TotalHits.Relation.EQUAL_TO), 1.5f);
        InternalSearchResponse sections = new InternalSearchResponse(
            hits,
            InternalAggregations.EMPTY,
            // core's Suggest sorts the list it is handed in place, so it cannot be an immutable one
            new Suggest(new ArrayList<>()),
            new SearchProfileShardResults(Map.of(SHARD_KEY, shardResult(1))),
            true,
            true,
            3,
            List.of(new RerankSearchExtBuilder(Map.of("query_text", "cat"))),
            List.of(new ProcessorExecutionDetail("normalization-processor"))
        );
        return new SearchResponse(
            sections,
            scrollId,
            5,
            4,
            1,
            17L,
            new SearchResponse.PhaseTook(Map.of("query", 11L)),
            new ShardSearchFailure[] { new ShardSearchFailure(new IllegalStateException("shard 4 is unavailable")) },
            SearchResponse.Clusters.EMPTY,
            pointInTimeId
        );
    }

    /** A response that has not timed out, carrying the given profile section — or none at all when it is {@code null}. */
    private static SearchResponse responseWithProfile(final Map<String, ProfileShardResult> profiles) {
        InternalSearchResponse sections = new InternalSearchResponse(
            SearchHits.empty(),
            InternalAggregations.EMPTY,
            null,
            Objects.isNull(profiles) ? null : new SearchProfileShardResults(profiles),
            false,
            null,
            1
        );
        return new SearchResponse(sections, null, 1, 1, 0, 7L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /** A recognizable shard result: a {@code nodes}-node query tree, and the same tree in the fetch and aggregation slots. */
    private static ProfileShardResult shardResult(final int nodes) {
        List<ProfileResult> tree = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            tree.add(new ProfileResult("type_" + i, "description_" + i, Map.of(), Map.of(), i, List.of()));
        }
        QueryProfileShardResult query = new QueryProfileShardResult(
            tree,
            11L,
            new CollectorResult("leg_collector", "search_top_hits", 13L, List.of())
        );
        return new ProfileShardResult(
            List.of(query),
            new AggregationProfileShardResult(tree),
            new FetchProfileShardResult(tree),
            new NetworkTime(nodes, nodes)
        );
    }
}
