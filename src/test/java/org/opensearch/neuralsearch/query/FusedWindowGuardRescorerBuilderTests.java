/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.rescore.QueryRescoreMode;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link FusedWindowGuardRescorerBuilder} — the wire form that carries the request's rescorer chain to the shard wrapped
 * in the guard.
 *
 * <p>Two things matter most here and both are asserted below. The chain must survive a wire round trip intact, because
 * the shard rebuilds the user's rescorers from it and a dropped field would silently change their scoring. And the
 * builder's own {@code window_size} must be the maximum over the chain, because after the wrap this is the only rescore
 * context core can see and core sizes the shard's first-pass pool from the largest window across contexts — get that
 * wrong and a deep rescorer silently stops reaching the documents it was asked to.
 */
public class FusedWindowGuardRescorerBuilderTests extends OpenSearchTestCase {

    /** The round trip the shard depends on: name, window size, and every delegate field. */
    public void testWireRoundTrip_preservesTheWholeChain() throws IOException {
        QueryRescorerBuilder first = new QueryRescorerBuilder(new MatchAllQueryBuilder()).setQueryWeight(0.5f)
            .setRescoreQueryWeight(3.0f)
            .setScoreMode(QueryRescoreMode.Multiply);
        first.windowSize(17);
        QueryRescorerBuilder second = new QueryRescorerBuilder(new TermQueryBuilder("f", "v"));
        second.windowSize(40);

        FusedWindowGuardRescorerBuilder roundTripped = roundTrip(new FusedWindowGuardRescorerBuilder(List.of(first, second)));

        assertEquals(FusedWindowGuardRescorerBuilder.NAME, roundTripped.getWriteableName());
        assertEquals("window size stands in for the whole chain's depth", Integer.valueOf(40), roundTripped.windowSize());
        assertEquals(2, roundTripped.delegates().size());
        QueryRescorerBuilder delegate = roundTripped.delegates().get(0);
        assertEquals(new MatchAllQueryBuilder(), delegate.getRescoreQuery());
        assertEquals(0.5f, delegate.getQueryWeight(), 0.0f);
        assertEquals(3.0f, delegate.getRescoreQueryWeight(), 0.0f);
        assertEquals(QueryRescoreMode.Multiply, delegate.getScoreMode());
        assertEquals(Integer.valueOf(17), delegate.windowSize());
        assertEquals("a delegate that set no window keeps none", Integer.valueOf(40), roundTripped.delegates().get(1).windowSize());
    }

    /**
     * Core sizes the pool from the widest window across rescore contexts. After the wrap there is one context, so the
     * guard has to report the chain's widest — not the first, and not its own default.
     */
    public void testWindowSize_isTheMaximumOverTheChain() {
        QueryRescorerBuilder narrow = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        narrow.windowSize(5);
        QueryRescorerBuilder wide = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        wide.windowSize(500);

        assertEquals(Integer.valueOf(500), new FusedWindowGuardRescorerBuilder(List.of(narrow, wide)).windowSize());
        assertEquals(Integer.valueOf(500), new FusedWindowGuardRescorerBuilder(List.of(wide, narrow)).windowSize());
    }

    /** A chain that names no window leaves the guard's unset too, so core applies its own default rather than a guess. */
    public void testWindowSize_whenNoDelegateSetsOne_thenItStaysUnset() {
        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
        );

        assertNull(guard.windowSize());
    }

    /**
     * A delegate that names no {@code window_size} is not windowless — core substitutes
     * {@code RescorerBuilder.DEFAULT_WINDOW_SIZE} for it. Since the guard is the only context core can see, counting only
     * the DECLARED windows would report 5 for this chain and have the shard collect a 5-document pool where the same chain
     * unwrapped collects 10, so the defaulting delegate would silently stop reaching documents it is supposed to rescore.
     */
    public void testWindowSize_whenADelegateLeavesItUnset_thenTheDefaultStillCounts() {
        QueryRescorerBuilder declared = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        declared.windowSize(5);

        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(declared, new QueryRescorerBuilder(new TermQueryBuilder("f", "v")))
        );

        assertEquals(
            "an unset delegate window is DEFAULT_WINDOW_SIZE, not zero",
            Integer.valueOf(RescorerBuilder.DEFAULT_WINDOW_SIZE),
            guard.windowSize()
        );
    }

    /** A declared window above the default still wins — the rule is the maximum, not the default. */
    public void testWindowSize_whenADeclaredWindowExceedsTheDefault_thenItWins() {
        QueryRescorerBuilder declared = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        declared.windowSize(64);

        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(declared, new QueryRescorerBuilder(new TermQueryBuilder("f", "v")))
        );

        assertEquals(Integer.valueOf(64), guard.windowSize());
    }

    /**
     * Two core fetch/search phases read the request's rescore contexts for their QUERIES, not their rescorers:
     * {@code MatchedQueriesPhase} collects {@code getParsedQueries().namedFilters()} for {@code matched_queries}, and
     * {@code DfsPhase} collects term statistics from the same list. After the wrap this context is the only one they can
     * see, and a bare {@code RescoreContext} answers both with an empty list — so a {@code _name} on a rescore query would
     * disappear, and under {@code dfs_query_then_fetch} the rescore query would score on local instead of global IDF.
     */
    public void testBuildContext_reportsEveryDelegatesQueryToTheCorePhasesThatReadThem() throws IOException {
        QueryShardContext shardContext = mock(QueryShardContext.class);
        when(shardContext.toQuery(org.mockito.ArgumentMatchers.any(QueryBuilder.class))).thenReturn(
            new ParsedQuery(new MatchAllDocsQuery())
        );
        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()), new QueryRescorerBuilder(new TermQueryBuilder("f", "v")))
        );

        RescoreContext context = guard.buildContext(shardContext);

        assertEquals("one parsed query per delegate, or matched_queries loses the name", 2, context.getParsedQueries().size());
        assertEquals("and the same for the DFS term-statistics walk", 2, context.getQueries().size());
    }

    /**
     * The three band constants, pinned against each other rather than against literals. Nothing may be representable
     * between the unranked sentinel and the ranked floor, or an extreme weight could land a ranked document inside the
     * unranked band; and the value the coordinator reports a floored ranked document as has to be both strictly above
     * the {@code 0.0} it reports an unranked one as, and the same floor a fused score already carries with no rescore in
     * the request — otherwise the guard would invent a score rather than preserve one.
     */
    public void testBandConstants_cannotMeetAndDecodeToTheNoRescoreScores() {
        assertEquals(
            "nothing may sit between the two bands",
            FusedWindowGuardRescorerBuilder.RANKED_FLOOR,
            Math.nextUp(FusedWindowGuardRescorerBuilder.UNRANKED_SCORE),
            0.0f
        );
        assertTrue(
            "a decoded ranked document must outrank a decoded unranked one",
            FusedWindowGuardRescorerBuilder.RANKED_FLOOR_NORMALIZED > 0.0f
        );
        assertEquals(
            "the decoded ranked floor is the floor a fused score already has without a rescore",
            HybridFusionOrchestrator.MIN_RANKED_SCORE,
            FusedWindowGuardRescorerBuilder.RANKED_FLOOR_NORMALIZED,
            0.0f
        );
    }

    /** Nothing to guard is a programming error in the install path, not a request a user can send. */
    public void testConstructor_rejectsAnEmptyChain() {
        expectThrows(IllegalArgumentException.class, () -> new FusedWindowGuardRescorerBuilder(List.of()));
        expectThrows(IllegalArgumentException.class, () -> new FusedWindowGuardRescorerBuilder((List<QueryRescorerBuilder>) null));
    }

    /** Rendered, never parsed: the name is there so an operator seeing it in a slow log knows whose it is. */
    public void testXContent_rendersTheGuardAndItsDelegates() throws IOException {
        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(new QueryRescorerBuilder(new TermQueryBuilder("colour", "hot")))
        );

        // An array, not an object: RescorerBuilder#toXContent opens its OWN anonymous object, which is only legal inside
        // an array — and an array is exactly how core renders the request's `rescore` list. Wrapping it in an object
        // instead throws "Cannot start an object, expecting a property name".
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        guard.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endArray();
        String rendered = builder.toString();

        assertTrue(rendered, rendered.contains(FusedWindowGuardRescorerBuilder.NAME));
        assertTrue("the user's own rescore query is still visible in the rendering", rendered.contains("colour"));
    }

    /** equals/hashCode have to see the chain, or a round-trip equality test would pass on a dropped delegate. */
    public void testEqualsAndHashCode_dependOnTheChainAndTheWindow() {
        QueryBuilder query = new TermQueryBuilder("f", "v");
        FusedWindowGuardRescorerBuilder one = new FusedWindowGuardRescorerBuilder(List.of(new QueryRescorerBuilder(query)));
        FusedWindowGuardRescorerBuilder same = new FusedWindowGuardRescorerBuilder(List.of(new QueryRescorerBuilder(query)));
        FusedWindowGuardRescorerBuilder different = new FusedWindowGuardRescorerBuilder(
            List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
        );

        assertEquals(one, same);
        assertEquals(one.hashCode(), same.hashCode());
        assertNotEquals(one, different);
        assertNotEquals(one, null);
        assertNotEquals(one, "not a rescorer");
    }

    /**
     * {@code buildContext} is what the shard actually calls, and it is where the chain becomes a guard wrapping one
     * {@link org.opensearch.search.rescore.RescoreContext} per delegate. The only thing core's
     * {@code QueryRescorerBuilder#innerBuildContext} needs from the shard context is {@code toQuery}, so a mock is enough
     * to reach it.
     */
    public void testBuildContext_wrapsOneContextPerDelegateAndCarriesTheWindowSize() throws IOException {
        QueryShardContext shardContext = mock(QueryShardContext.class);
        when(shardContext.toQuery(org.mockito.ArgumentMatchers.any(QueryBuilder.class))).thenReturn(
            new ParsedQuery(new MatchAllDocsQuery())
        );
        QueryRescorerBuilder first = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        first.windowSize(31);
        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(first, new QueryRescorerBuilder(new TermQueryBuilder("f", "v")))
        );

        RescoreContext context = guard.buildContext(shardContext);

        assertEquals("the guard's own window stands in for the chain's depth", 31, context.getWindowSize());
        assertTrue(context.rescorer() instanceof FusedWindowGuardRescorer);
        assertEquals(2, ((FusedWindowGuardRescorer) context.rescorer()).delegates().size());
    }

    /**
     * The path that matters for a rescore query which rewrites — a {@code wrapper}, a {@code neural} query, a
     * {@code terms} lookup. A rewritten delegate has to produce a NEW guard carrying it, or the rewrite would be lost and
     * the shard would run the un-rewritten query.
     */
    public void testRewrite_whenADelegateRewrites_thenANewGuardCarriesTheRewrittenChain() throws IOException {
        FusedWindowGuardRescorerBuilder guard = new FusedWindowGuardRescorerBuilder(
            List.of(new QueryRescorerBuilder(new RewritesOnceQueryBuilder()))
        );
        guard.windowSize(12);

        RescorerBuilder<FusedWindowGuardRescorerBuilder> rewritten = guard.rewrite(mock(QueryRewriteContext.class));

        assertNotSame("a rewritten delegate must produce a new guard", guard, rewritten);
        assertEquals("the window size is carried across the rewrite", Integer.valueOf(12), rewritten.windowSize());
        QueryBuilder rescoreQuery = ((FusedWindowGuardRescorerBuilder) rewritten).delegates().get(0).getRescoreQuery();
        assertTrue("the delegate now holds the rewritten query", rescoreQuery instanceof MatchAllQueryBuilder);
    }

    /** Rewrites to a {@code match_all} on its first rewrite, so a chain containing it is guaranteed to change. */
    private static final class RewritesOnceQueryBuilder extends AbstractQueryBuilder<RewritesOnceQueryBuilder> {
        @Override
        protected QueryBuilder doRewrite(org.opensearch.index.query.QueryRewriteContext context) {
            return new MatchAllQueryBuilder();
        }

        @Override
        public String getWriteableName() {
            return "rewrites_once";
        }

        @Override
        protected void doWriteTo(org.opensearch.core.common.io.stream.StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getWriteableName()).endObject();
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("never executed");
        }

        @Override
        protected boolean doEquals(RewritesOnceQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    private FusedWindowGuardRescorerBuilder roundTrip(final FusedWindowGuardRescorerBuilder guard) throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            java.util.stream.Stream.concat(
                new SearchModule(org.opensearch.common.settings.Settings.EMPTY, List.of()).getNamedWriteables().stream(),
                java.util.stream.Stream.of(
                    new NamedWriteableRegistry.Entry(
                        RescorerBuilder.class,
                        FusedWindowGuardRescorerBuilder.NAME,
                        FusedWindowGuardRescorerBuilder::new
                    )
                )
            ).toList()
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            guard.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                return new FusedWindowGuardRescorerBuilder(in);
            }
        }
    }
}
