/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.neuralsearch.search.profile.FusedCoordinatorTimings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * {@link FusedLegResolver} on its own: the legs of a fused hybrid brought to their coordinator fixpoints ahead of the
 * fan-out, each exactly once, in leg order, with a failure attributed to the leg that failed.
 */
public class FusedLegResolverTests extends OpenSearchTestCase {

    private static final String FIELD = "field";

    private static final int WINDOW = 7;

    /** The request the legs belong to: sized at the user's page, which a leg's own request must NOT inherit. */
    private final SearchRequest searchRequest = new SearchRequest("index").source(new SearchSourceBuilder().size(3));

    /** What each leg inherits from that request, and what builds the leg request a leg is resolved against. */
    private final CandidateScope scope = CandidateScope.from(searchRequest);

    private final Client client = mock(Client.class);

    /** A leg that neither registers anything nor changes is handed back as itself, and runs at once. */
    public void testResolveAndRun_whenNoLegRewrites_thenEachLegIsHandedBackAsItselfAndRunsAtOnce() {
        TermQueryBuilder first = new TermQueryBuilder(FIELD, "a");
        MatchQueryBuilder second = new MatchQueryBuilder(FIELD, "b");
        List<SearchRequest> dispatched = answerSearchesAtOnce();

        List<QueryBuilder> resolved = resolveAndRun(List.of(first, second), parentContext()).resolvedLegs();

        assertSame("nothing changed, so the caller can tell by identity", first, resolved.get(0));
        assertSame(second, resolved.get(1));
        assertEquals("both legs were dispatched, nothing held them", 2, dispatched.size());
        assertSame(first, dispatched.get(0).source().query());
        assertSame(second, dispatched.get(1).source().query());
    }

    /**
     * The defect's mechanism, inverted: a leg that rewrites the way {@code neural} does — registering its model call and
     * returning a new builder — is driven to the form that call produces, and the call is made once. The resolved builder
     * inherits the leg's {@code _name} and boost, so what reports on the leg still finds them.
     */
    public void testResolveAndRun_whenALegRegistersItsInference_thenItIsResolvedOnceToItsFinalForm() {
        ResolvingLegQueryBuilder neuralLike = new ResolvingLegQueryBuilder(FIELD, "shoes");
        neuralLike.queryName("vector_leg").boost(2.0f);
        TermQueryBuilder lexical = new TermQueryBuilder(FIELD, "shoes");
        answerSearchesAtOnce();

        List<QueryBuilder> resolved = resolveAndRun(List.of(lexical, neuralLike), parentContext()).resolvedLegs();

        assertEquals(2, resolved.size());
        assertSame("a leg with nothing to resolve is the same instance", lexical, resolved.get(0));
        assertEquals(
            "the inferring leg arrives in its final form",
            neuralLike.resolvedForm().queryName("vector_leg").boost(2.0f),
            resolved.get(1)
        );
        assertEquals("the model was called exactly once", 1, neuralLike.inferences());
        assertNotSame("and the user's own builder was left as written", neuralLike, resolved.get(1));
    }

    /** Legs resolve concurrently and may answer in any order; the resolved list is still in leg order. */
    public void testResolveAndRun_whenLegsResolveOutOfOrder_thenTheResolvedListKeepsLegOrder() {
        ResolvingLegQueryBuilder first = new ResolvingLegQueryBuilder(FIELD, "first").holdActions();
        ResolvingLegQueryBuilder second = new ResolvingLegQueryBuilder(FIELD, "second").holdActions();
        answerSearchesAtOnce();
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();

        FusedLegResolver.resolveAndRun(
            List.of(first, second),
            parentContext(),
            client,
            scope,
            WINDOW,
            leg -> leg,
            new FusedCoordinatorTimings(),
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError(e);
            })
        );
        assertNull("nothing is handed back while a leg is still resolving", run.get());

        second.completePending();
        assertNull("the first leg has not answered yet", run.get());
        first.completePending();

        assertNotNull(run.get());
        assertEquals(List.of(first.resolvedForm(), second.resolvedForm()), run.get().resolvedLegs());
        assertEquals(1, first.inferences());
        assertEquals(1, second.inferences());
    }

    /**
     * Core's {@code rewriteAndFetch} routes an {@code IllegalArgumentException} to the listener and lets any other exception
     * escape; both become that leg's item failure, carrying the cause itself — {@code groupLegHits} frames it with the leg's
     * index and the cause's own status — and the sibling runs regardless.
     */
    public void testResolveAndRun_whenALegThrowsWhileRewriting_thenItIsThatLegsItemFailureWhetherOrNotCoreRoutesIt() {
        IllegalArgumentException badBound = new IllegalArgumentException("bad bound");
        List<SearchRequest> dispatched = answerSearchesAtOnce();
        MultiSearchResponse.Item[] routed = resolveAndRun(
            List.of(new TermQueryBuilder(FIELD, "a"), new ThrowingLegQueryBuilder(badBound)),
            parentContext()
        ).responses().getResponses();
        assertFalse("the sibling ran", routed[0].isFailure());
        assertSame("the routed exception is the leg's item failure", badBound, routed[1].getFailure());
        assertEquals("only the sibling was dispatched", 1, dispatched.size());

        IllegalStateException brokenBuilder = new IllegalStateException("broken builder");
        MultiSearchResponse.Item[] escaped = resolveAndRun(
            List.of(new ThrowingLegQueryBuilder(brokenBuilder), new TermQueryBuilder(FIELD, "a")),
            parentContext()
        ).responses().getResponses();
        assertSame("the escaping exception is the leg's item failure too", brokenBuilder, escaped[0].getFailure());
        assertFalse(escaped[1].isFailure());
    }

    /**
     * Resolution is generic: any leg with coordinator-side work is brought to its fixpoint, not only an inferring one. A
     * {@code wrapper} leg is parsed into the query it holds, with the parent context's registry, and that is what runs.
     */
    public void testResolveAndRun_whenALegIsAWrapper_thenItIsParsedIntoTheQueryItHolds() {
        QueryRewriteContext parent = parentContext();
        when(parent.getXContentRegistry()).thenReturn(queryParsingRegistry());
        WrapperQueryBuilder wrapper = new WrapperQueryBuilder("{\"match\":{\"" + FIELD + "\":\"hello\"}}");
        List<SearchRequest> dispatched = answerSearchesAtOnce();

        List<QueryBuilder> resolved = resolveAndRun(List.of(wrapper), parent).resolvedLegs();

        assertEquals(List.of(new MatchQueryBuilder(FIELD, "hello")), resolved);
        assertEquals("the parsed query is what ran", new MatchQueryBuilder(FIELD, "hello"), dispatched.get(0).source().query());
    }

    /**
     * A leg that is itself a fused hybrid is left exactly as written and runs as its own leg sub-search's request query,
     * where it fans out on its own; resolving it here would fan it out on this coordinator instead.
     */
    public void testResolveAndRun_whenALegIsAFusedHybrid_thenItIsLeftAsWrittenAndRunsAsItsOwnSubSearch() {
        HybridQueryBuilder nested = new HybridQueryBuilder().add(new TermQueryBuilder(FIELD, "a")).add(new TermQueryBuilder(FIELD, "b"));
        nested.fusion(new HashMap<>(Map.of("normalization", Map.of("technique", "min_max"))));
        TermQueryBuilder lexical = new TermQueryBuilder(FIELD, "c");
        List<SearchRequest> dispatched = answerSearchesAtOnce();

        List<QueryBuilder> resolved = resolveAndRun(List.of(nested, lexical), parentContext()).resolvedLegs();

        assertSame("the nested fused hybrid is the same instance", nested, resolved.get(0));
        assertSame(lexical, resolved.get(1));
        assertSame("and it is what its leg sub-search runs", nested, dispatched.get(0).source().query());
        verify(client, never()).multiSearch(any(), any());
    }

    /** The marker is not applied where no fan-out can happen, and never stacks on itself. */
    public void testLegPreparationContext_whenThereIsNothingToMark_thenTheContextIsUnchanged() {
        QueryRewriteContext plainContext = mock(QueryRewriteContext.class);
        assertSame(
            "a shard-side rewrite has no coordinator context to wrap",
            plainContext,
            LegPreparationRewriteContext.wrap(plainContext)
        );
        assertFalse(LegPreparationRewriteContext.isPreparingLegs(plainContext));

        QueryRewriteContext marked = LegPreparationRewriteContext.wrap(parentContext());
        assertTrue(LegPreparationRewriteContext.isPreparingLegs(marked));
        assertSame("a leg being prepared inside a leg being prepared is already marked", marked, LegPreparationRewriteContext.wrap(marked));
        assertSame(
            "the marked context reports the request it was wrapped for",
            searchRequest,
            marked.convertToCoordinatorContext().getSearchRequest()
        );
    }

    /**
     * The request a leg sees while it resolves is its OWN — the one its sub-search will run, sized at the window — and not
     * the enclosing request sized at the user's page. k-NN's radial rewrite reads {@code source().size()} off it and bakes
     * the value in as the leg's per-shard cap, so handing a leg the enclosing request would have shrunk a radial leg's
     * candidate set to the page without the leg sub-search being able to correct it.
     */
    public void testResolveAndRun_whenALegReadsItsRequest_thenItSeesTheLegRequestSizedAtTheWindow() {
        RequestRecordingLegQueryBuilder recording = new RequestRecordingLegQueryBuilder();
        answerSearchesAtOnce();

        resolveAndRun(List.of(new TermQueryBuilder(FIELD, "a"), recording), parentContext());

        SearchRequest seen = recording.seen;
        assertNotNull("the leg was resolved under a coordinator context carrying a request", seen);
        assertNotSame("not the enclosing request", searchRequest, seen);
        assertEquals("the leg request is sized at the window, as the leg sub-search is", WINDOW, seen.source().size());
        assertEquals("over the request's indices", List.of("index"), List.of(seen.indices()));
        assertEquals("with the search pipeline off, as a leg runs", "_none", seen.pipeline());
        assertSame("the leg request carries the leg itself", recording, seen.source().query());
    }

    // ------------------------------------------- resolve and run: overlapped dispatch -------------------------------------------

    /**
     * Each leg runs the moment it is resolved: with the inferring leg held back, the lexical leg's search is already dispatched
     * — the overlap the legs had when each resolved inside its own sub-search — and the inferring leg's search is dispatched
     * only when its "inference" answers. The responses come back in leg order whatever the order of completion, in the
     * MultiSearchResponse shape the fan-out always produced, alongside the resolved legs.
     */
    public void testResolveAndRun_whenOneLegInfers_thenTheOtherLegRunsMeanwhileAndResponsesKeepLegOrder() {
        ResolvingLegQueryBuilder neuralLike = new ResolvingLegQueryBuilder(FIELD, "shoes").holdActions();
        TermQueryBuilder lexical = new TermQueryBuilder(FIELD, "shoes");
        List<SearchRequest> dispatched = new ArrayList<>();
        Map<SearchRequest, ActionListener<SearchResponse>> pending = new LinkedHashMap<>();
        doAnswer(invocation -> {
            dispatched.add(invocation.getArgument(0));
            pending.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(client).search(any(), any());
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings();
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();

        FusedLegResolver.resolveAndRun(
            List.of(neuralLike, lexical),
            parentContext(),
            client,
            scope,
            WINDOW,
            leg -> leg,
            timings,
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError(e);
            })
        );

        assertEquals("the lexical leg was dispatched at once, while the other leg is still inferring", 1, dispatched.size());
        assertSame(lexical, dispatched.get(0).source().query());
        assertEquals("sized at the window, as a leg sub-search is", WINDOW, dispatched.get(0).source().size());
        // The lexical leg answers before the inference does.
        pending.get(dispatched.get(0)).onResponse(response(2));
        assertNull("nothing is handed back while a leg is still resolving", run.get());

        neuralLike.completePending();
        assertEquals("the inferring leg was dispatched once its vector was back", 2, dispatched.size());
        assertEquals("as its resolved form", neuralLike.resolvedForm(), dispatched.get(1).source().query());
        assertNull("and nothing is handed back until it answers", run.get());
        pending.get(dispatched.get(1)).onResponse(response(3));

        assertNotNull(run.get());
        assertEquals("resolved legs in leg order", List.of(neuralLike.resolvedForm(), lexical), run.get().resolvedLegs());
        MultiSearchResponse.Item[] items = run.get().responses().getResponses();
        assertEquals(2, items.length);
        assertEquals("responses in leg order, not completion order", 3, items[0].getResponse().getHits().getHits().length);
        assertEquals(2, items[1].getResponse().getHits().getHits().length);
        assertEquals("the model was called once", 1, neuralLike.inferences());
        assertTrue("the resolution span ran until the last leg was dispatched", timings.legsResolveWaitNanos() > 0);
        assertTrue("and the three fan-out spans are closed", timings.fanOutWaitNanos() >= 0 && timings.fanOutBuildNanos() > 0);
    }

    /**
     * A leg that cannot be resolved does not stop the others from running, and is reported as that leg's item failure — the
     * shape {@code groupLegHits} turns into {@code [hybrid] fused-mode sub-query N failed: …} with the leg's own status — once
     * every leg has answered. The failed leg is carried as written.
     */
    public void testResolveAndRun_whenALegFailsToResolve_thenItIsAnItemFailureAndTheOthersStillRun() {
        OpenSearchStatusException modelMissing = new OpenSearchStatusException("model [m1] not found", RestStatus.NOT_FOUND);
        ResolvingLegQueryBuilder failing = new ResolvingLegQueryBuilder(FIELD, "shoes").failWith(modelMissing);
        TermQueryBuilder lexical = new TermQueryBuilder(FIELD, "shoes");
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(response(2));
            return null;
        }).when(client).search(any(), any());
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();

        FusedLegResolver.resolveAndRun(
            List.of(lexical, failing),
            parentContext(),
            client,
            scope,
            WINDOW,
            leg -> leg,
            new FusedCoordinatorTimings(),
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError(e);
            })
        );

        assertNotNull("every leg answered", run.get());
        MultiSearchResponse.Item[] items = run.get().responses().getResponses();
        assertFalse("the lexical leg ran and answered", items[0].isFailure());
        assertTrue("the failing leg is an item failure", items[1].isFailure());
        assertSame(modelMissing, items[1].getFailure());
        assertSame("carried as written", failing, run.get().resolvedLegs().get(1));
        verify(client, times(1)).search(any(), any());
    }

    /** A leg whose search fails is that leg's item failure too, with the search's own exception. */
    public void testResolveAndRun_whenALegSearchFails_thenItIsAnItemFailure() {
        TermQueryBuilder first = new TermQueryBuilder(FIELD, "a");
        TermQueryBuilder second = new TermQueryBuilder(FIELD, "b");
        RuntimeException rejected = new RuntimeException("search rejected");
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (request.source().query() == second) {
                listener.onFailure(rejected);
            } else {
                listener.onResponse(response(1));
            }
            return null;
        }).when(client).search(any(), any());
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();

        FusedLegResolver.resolveAndRun(
            List.of(first, second),
            parentContext(),
            client,
            scope,
            WINDOW,
            leg -> leg,
            new FusedCoordinatorTimings(),
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError(e);
            })
        );

        MultiSearchResponse.Item[] items = run.get().responses().getResponses();
        assertFalse(items[0].isFailure());
        assertSame(rejected, items[1].getFailure());
    }

    /** The fan-out form of a leg — what the enclosing query hands a nested fused leg — is what runs, while the resolved leg is what is kept. */
    public void testResolveAndRun_runsTheFanOutFormButKeepsTheResolvedLeg() {
        TermQueryBuilder leg = new TermQueryBuilder(FIELD, "a");
        TermQueryBuilder projected = new TermQueryBuilder(FIELD, "a-projected");
        List<SearchRequest> dispatched = new ArrayList<>();
        doAnswer(invocation -> {
            dispatched.add(invocation.getArgument(0));
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(response(1));
            return null;
        }).when(client).search(any(), any());
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();

        FusedLegResolver.resolveAndRun(
            List.of(leg),
            parentContext(),
            client,
            scope,
            WINDOW,
            l -> l == leg ? projected : l,
            new FusedCoordinatorTimings(),
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError(e);
            })
        );

        assertSame("what ran is the fan-out form", projected, dispatched.get(0).source().query());
        assertSame("what is kept for the Tail is the resolved leg", leg, run.get().resolvedLegs().get(0));
    }

    private static SearchResponse response(final int hitCount) {
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = new SearchHit(i, String.valueOf(i), Collections.emptyMap(), Collections.emptyMap());
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(sections, null, 1, 1, 0, 1, null, null);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Resolve-and-run with the client already answering every leg search; everything completes synchronously, so the run is
     * in hand when this returns. Legs whose searches must stay pending drive {@link FusedLegResolver#resolveAndRun} directly.
     */
    private FusedLegResolver.LegsRun resolveAndRun(final List<QueryBuilder> legs, final QueryRewriteContext parent) {
        AtomicReference<FusedLegResolver.LegsRun> run = new AtomicReference<>();
        FusedLegResolver.resolveAndRun(
            legs,
            parent,
            client,
            scope,
            WINDOW,
            leg -> leg,
            new FusedCoordinatorTimings(),
            ActionListener.wrap(run::set, e -> {
                throw new AssertionError("the fan-out's own machinery failed: " + e.getMessage(), e);
            })
        );
        assertNotNull("every leg answered synchronously, so the run is in hand", run.get());
        return run.get();
    }

    /** Every leg search the client receives answers an empty page at once; the requests, in dispatch order. */
    private List<SearchRequest> answerSearchesAtOnce() {
        List<SearchRequest> dispatched = new ArrayList<>();
        doAnswer(invocation -> {
            dispatched.add(invocation.getArgument(0));
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(response(0));
            return null;
        }).when(client).search(any(), any());
        return dispatched;
    }

    /** The context the hybrid is being rewritten under: a coordinator context for {@link #searchRequest}. */
    private QueryCoordinatorContext parentContext() {
        QueryCoordinatorContext parent = mock(QueryCoordinatorContext.class);
        when(parent.convertToCoordinatorContext()).thenReturn(parent);
        when(parent.getSearchRequest()).thenReturn(searchRequest);
        return parent;
    }

    private NamedXContentRegistry queryParsingRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(MatchQueryBuilder.NAME), MatchQueryBuilder::fromXContent)
            )
        );
    }

    /** A leg that records the request the coordinator context it is rewritten under reports, as k-NN's radial rewrite reads it. */
    private static final class RequestRecordingLegQueryBuilder extends AbstractQueryBuilder<RequestRecordingLegQueryBuilder> {
        private SearchRequest seen;

        @Override
        protected QueryBuilder doRewrite(final QueryRewriteContext queryRewriteContext) {
            QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
            if (coordinatorContext != null) {
                seen = (SearchRequest) coordinatorContext.getSearchRequest();
            }
            return this;
        }

        @Override
        protected void doWriteTo(final StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("recording_leg").endObject();
        }

        @Override
        protected Query doToQuery(final QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doEquals(final RequestRecordingLegQueryBuilder other) {
            return this == other;
        }

        @Override
        protected int doHashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public String getWriteableName() {
            return "recording_leg";
        }
    }

    /** A leg whose rewrite throws, standing for a builder that refuses its own input. */
    private static final class ThrowingLegQueryBuilder extends AbstractQueryBuilder<ThrowingLegQueryBuilder> {
        private final RuntimeException failure;

        private ThrowingLegQueryBuilder(final RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        protected QueryBuilder doRewrite(final QueryRewriteContext queryRewriteContext) {
            throw failure;
        }

        @Override
        protected void doWriteTo(final StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("throwing_leg").endObject();
        }

        @Override
        protected Query doToQuery(final QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doEquals(final ThrowingLegQueryBuilder other) {
            return failure == other.failure;
        }

        @Override
        protected int doHashCode() {
            return failure.hashCode();
        }

        @Override
        public String getWriteableName() {
            return "throwing_leg";
        }
    }
}
