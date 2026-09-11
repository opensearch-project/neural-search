/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescoreMode;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

/**
 * A {@code rescore} on a fused {@code hybrid} query may reorder the hybrid's hits but never add to them, which
 * {@link FusedRescoreScope} enforces by confining every rescore query to the fused window.
 *
 * <p>Two things are being pinned here, and they need different kinds of test. <b>What</b> the confinement is —
 * {@code bool{must: <the user's query>, filter: <the window>}}, weights and window size untouched, the window
 * {@code _index}-qualified — is ordinary state assertion. <b>Whether it reaches the shards</b> is not: it depends entirely
 * on core's own rewrite bookkeeping, so those tests drive {@code SearchSourceBuilder#rewrite} the way
 * {@code Rewriteable#rewriteAndFetch} does and assert against the source that pass produces, never against the source the
 * plugin wrote to. That distinction is the whole reason this class exists — an earlier implementation confined the rescore
 * correctly and still shipped nothing, because it wrote to a list core had already replaced.
 */
public class FusedRescoreScopeTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";

    // ---- what the confinement looks like ----

    /**
     * The defect this closes. {@code rescore} is never propagated to a leg, so it runs where core always runs it: once per
     * shard, against the self-erased {@code bool{Top + Tail}}. The Tail is a {@code filter}, so every document any leg
     * matched is a rescore candidate there — sitting at the floor, but still in the pool core re-scores — and a rescore
     * query matching one lifts a document fusion never ranked into the results. Intersecting the rescore query with the
     * fused window makes such a document unliftable no matter how deep the rescore window reaches.
     */
    public void testInstall_confinesTheRescoreQueryToTheWindow() throws IOException {
        MatchQueryBuilder declaredQuery = new MatchQueryBuilder("text", "hot");
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(declaredQuery));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(fusedOver("1", "2"));

        BoolQueryBuilder confined = confinedQueryOf(coreRewriteToFixedPoint(source));
        assertEquals("the user's query stays the sole scoring clause, so its contribution is unchanged", 1, confined.must().size());
        assertSame(declaredQuery, confined.must().get(0));
        assertEquals("the window rides along as a non-scoring filter", 1, confined.filter().size());
        assertAddressedTo(confined.filter().get(0), INDEX, "1", "2");
    }

    /**
     * Everything about the rescorer other than its query decides how the rescore combines with the fused score and how deep
     * it reaches, none of which confining the query has any business changing. Read off the rewritten source rather than the
     * installed one, because when the placeholder resolves it is core's {@code QueryRescorerBuilder#rewrite} that rebuilds
     * the rescorer — so this asserts core carries them too, not just that the plugin copied them.
     */
    public void testInstall_preservesWeightsScoreModeAndWindowSize() throws IOException {
        QueryRescorerBuilder declared = new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")).setQueryWeight(0.3f)
            .setRescoreQueryWeight(7.0f)
            .setScoreMode(QueryRescoreMode.Multiply);
        declared.windowSize(42);
        SearchSourceBuilder source = sourceWithRescorer(declared);

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(fusedOver("1"));

        QueryRescorerBuilder dispatched = rescorerOf(coreRewriteToFixedPoint(source));
        assertEquals(0.3f, dispatched.getQueryWeight(), 0.0f);
        assertEquals(7.0f, dispatched.getRescoreQueryWeight(), 0.0f);
        assertEquals(QueryRescoreMode.Multiply, dispatched.getScoreMode());
        assertEquals(Integer.valueOf(42), dispatched.windowSize());
    }

    /** An unset {@code window_size} means core's default, so it must stay unset rather than being pinned to a number. */
    public void testInstall_leavesAnUnsetWindowSizeUnset() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(fusedOver("1"));

        assertNull(rescorerOf(coreRewriteToFixedPoint(source)).windowSize());
    }

    /**
     * A multi-index window has to be addressed per index, exactly as the Top addresses it: {@code _id} is unique only within
     * an index, so an {@code _id}-only filter would admit a sibling index's same-{@code _id} document — a document outside
     * the window — back into the rescore, which is the very thing being closed.
     */
    public void testInstall_whenWindowSpansIndices_thenFilterIsIndexQualifiedPerIndex() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(
            new HybridFusionQueryBuilder(
                new String[] { "1", "2", "1" },
                new String[] { "idx-a", "idx-a", "idx-b" },
                new float[] { 0.9f, 0.8f, 0.7f },
                List.of()
            )
        );

        BoolQueryBuilder perIndex = (BoolQueryBuilder) confinedQueryOf(coreRewriteToFixedPoint(source)).filter().get(0);
        assertEquals("one OR-ed clause per index in the window", 2, perIndex.should().size());
        assertAddressedTo(perIndex.should().get(0), "idx-a", "1", "2");
        assertAddressedTo(perIndex.should().get(1), "idx-b", "1");
        assertTrue(
            "no clause may address an _id without its _index",
            perIndex.should().stream().noneMatch(clause -> clause instanceof IdsQueryBuilder)
        );
    }

    /** Every rescorer in the chain is confined, not just the first — core applies them in sequence and each one can lift. */
    public void testInstall_confinesEveryRescorerInTheChain() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        source.addRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "fresh")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(fusedOver("1"));

        SearchSourceBuilder dispatched = coreRewriteToFixedPoint(source);
        assertEquals("the chain is carried inside one guard element", 1, dispatched.rescores().size());
        assertEquals(2, ((FusedWindowGuardRescorerBuilder) dispatched.rescores().get(0)).delegates().size());
        // Both of the user's rescorers, not both list elements: after the wrap there is one element and the chain lives
        // inside it, so the loop walks the guard's delegates.
        for (int i = 0; i < 2; i++) {
            BoolQueryBuilder confined = (BoolQueryBuilder) rescorerOf(dispatched, i).getRescoreQuery();
            assertAddressedTo(confined.filter().get(0), INDEX, "1");
        }
    }

    /** A request without a rescore, an empty {@code "rescore": []}, and a null source are all nothing to install into. */
    public void testInstall_withoutARescoreOrSource_isANoOp() {
        assertNull(FusedRescoreScope.install(null));
        assertNull(FusedRescoreScope.install(new SearchSourceBuilder()));

        SearchSourceBuilder emptyList = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        emptyList.rescores().clear();
        assertNull(FusedRescoreScope.install(emptyList));
        assertTrue(emptyList.rescores().isEmpty());
    }

    /**
     * Confining a rescore means rewriting its query, which only core's {@code query} rescorer exposes. A rescorer type
     * registered by another plugin is refused rather than passed through: passing it through would leave exactly the defect
     * being closed, silently and only for that rescorer.
     *
     * <p>Refused at install time, which is what makes the refusal depend on the request alone. Deciding it later — when the
     * window is known — would refuse the same request only when the fusion happened to rank at least one document.
     */
    public void testInstall_whenRescorerIsNotAQueryRescorer_thenRefused() {
        SearchSourceBuilder source = sourceWithRescorer(new UnsupportedRescorerBuilder());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FusedRescoreScope.install(source));

        assertTrue(e.getMessage(), e.getMessage().contains("does not support rescorer [not_a_query_rescorer]"));
        assertTrue(e.getMessage(), e.getMessage().contains("only the [query] rescorer exposes one"));
    }

    /**
     * And the refusal is all-or-nothing. {@code source.rescores()} is the request's own live list, so confining element by
     * element would leave a refused request carrying a rescorer whose placeholder no scope will ever resolve. Core discards
     * the source on a rewrite failure today, so nothing dispatches it — but that is core's error path, not this class's
     * invariant, and a task description rendering the live source mid-refusal already sees it.
     */
    public void testInstall_whenALaterRescorerIsNotAQueryRescorer_thenNothingIsReplaced() {
        QueryRescorerBuilder supported = new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot"));
        SearchSourceBuilder source = sourceWithRescorer(supported);
        source.addRescorer(new UnsupportedRescorerBuilder());

        expectThrows(IllegalArgumentException.class, () -> FusedRescoreScope.install(source));

        assertEquals(2, source.rescores().size());
        assertSame("the supported rescorer is left exactly as the user wrote it", supported, source.rescores().get(0));
    }

    // ---- whether the confinement reaches the shards ----

    /**
     * The reason the window arrives as a placeholder instead of being written in from the fusion callback. The user's rescore
     * query rewrites on the first pass — {@code wrapper}, {@code neural}, {@code neural_sparse} and a {@code terms} lookup
     * all do — so {@code QueryRescorerBuilder#rewrite} returns a new builder, {@code Rewriteable#rewrite(List, ...)} returns
     * a new {@code ArrayList}, and {@code SearchSourceBuilder#rewrite} hands that new list to {@code shallowCopy}. From then
     * on {@code source.rescores()} is not the list being dispatched, and anything written to it is lost.
     *
     * <p>What survives that is the placeholder <i>object</i>: core copies it into the new list clause by clause, so the pass
     * after the fan-out resolves it inside core's own copy. Asserted as the identity chain, because the identity is the
     * mechanism.
     */
    public void testInstall_whenTheRescoreQueryRewrites_thenTheConfinementStillReachesTheDispatchedSource() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new RewritesOnceQueryBuilder()));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        QueryBuilder placeholder = confinedQueryOf(source).filter().get(0);
        // Pass 1: the pass that would have fired the legs. Core replaces the rescore list because the user's query rewrote.
        SearchSourceBuilder afterFanOut = source.rewrite(rewriteContext());
        assertNotSame("core rebuilt the rescore list, so the plugin's own list is now orphaned", source.rescores(), afterFanOut.rescores());
        assertNotSame(source.rescores().get(0), afterFanOut.rescores().get(0));
        assertSame("but the placeholder itself was carried across", placeholder, confinedQueryOf(afterFanOut).filter().get(0));

        // The leg MultiSearch comes back.
        scope.resolve(fusedOver("1", "2"));
        scope.requireReachedTheExecutedRequest();

        BoolQueryBuilder confined = confinedQueryOf(coreRewriteToFixedPoint(afterFanOut));
        assertAddressedTo(confined.filter().get(0), INDEX, "1", "2");
        assertEquals("the rewritten user query is still the only scoring clause", 1, confined.must().size());
        assertEquals(new MatchQueryBuilder("text", "rewritten"), confined.must().get(0));
    }

    /**
     * The fail-closed half. If the source the confinement was installed into is not the one core rewrites — the shape a
     * search-pipeline request processor handing back a different {@code SearchRequest} with a deep-copied source would
     * produce — then no placeholder is ever visited, and the request must fail rather than answer with a rescore that can
     * reach outside the fused window.
     */
    public void testRequireReachedTheExecutedRequest_whenCoreNeverRewroteThePlaceholder_thenFailsClosed() {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(fusedOver("1"));

        IllegalStateException e = expectThrows(IllegalStateException.class, scope::requireReachedTheExecutedRequest);
        assertTrue(e.getMessage(), e.getMessage().contains("the [rescore] this coordinator rewrote is not the one being executed"));
    }

    /** And it passes as soon as core has taken its pass over the source the placeholders were installed into. */
    public void testRequireReachedTheExecutedRequest_afterCoresOwnPass_thenSatisfied() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        source.rewrite(rewriteContext());
        scope.resolve(fusedOver("1"));

        scope.requireReachedTheExecutedRequest();
    }

    /**
     * Every rescorer's placeholder has to be checked, not just one: a request with two rescorers whose first was somehow
     * rewritten and second was not is still a request with an unconfined rescore.
     */
    public void testRequireReachedTheExecutedRequest_checksEveryPlaceholderInTheChain() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        source.addRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "fresh")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        // Only the first rescorer is offered to core, which is what a partially-copied source would look like.
        rescorerOf(source, 0).getRescoreQuery().rewrite(rewriteContext());
        scope.resolve(fusedOver("1"));

        expectThrows(IllegalStateException.class, scope::requireReachedTheExecutedRequest);
    }

    // ---- the placeholder itself ----

    /**
     * Nothing fused means round 2 is a {@code match_none} with no hits for a rescore to reorder — but the placeholder still
     * has to resolve, since an unresolved one would travel to the shards. An empty window is {@code match_none}, and core's
     * own {@code BoolQueryBuilder} rewrite then collapses the whole rescore query to {@code match_none}, which is exactly
     * right: there is nothing a rescore is allowed to match.
     */
    public void testResolve_whenNothingFused_thenTheRescoreQueryMatchesNothing() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope scope = FusedRescoreScope.install(source);
        scope.resolve(new MatchNoneQueryBuilder());

        assertTrue(rescorerOf(coreRewriteToFixedPoint(source)).getRescoreQuery() instanceof MatchNoneQueryBuilder);
    }

    /**
     * Before the fan-out returns there is nothing to resolve to, and the placeholder has to stay put — a rewrite loop only
     * continues while the builder's identity changes, so a placeholder that rewrote to anything else here would be gone
     * before the window existed.
     */
    public void testPlaceholder_beforeTheWindowIsKnown_staysInPlace() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));

        FusedRescoreScope.install(source);
        QueryBuilder placeholder = confinedQueryOf(source).filter().get(0);

        assertSame(placeholder, placeholder.rewrite(rewriteContext()));
        assertSame(
            "and the rescorer around it is unchanged too, so core's loop terminates",
            placeholder,
            confinedQueryOf(coreRewriteToFixedPoint(source)).filter().get(0)
        );
    }

    /**
     * The placeholder is a coordinator-rewrite artifact: it is not registered as a query, is never parsed, and is resolved
     * before the request is dispatched. Both ways it could leave the coordinator throw rather than degrade, because either
     * one means the window never arrived.
     */
    public void testPlaceholder_isNeitherExecutableNorSerializable() {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        FusedRescoreScope.install(source);
        QueryBuilder placeholder = confinedQueryOf(source).filter().get(0);

        assertEquals("hybrid_fused_window", placeholder.getWriteableName());
        assertTrue(
            expectThrows(IllegalStateException.class, () -> placeholder.toQuery(null)).getMessage().contains("must never be executed")
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            assertTrue(expectThrows(IllegalStateException.class, () -> placeholder.writeTo(out)).getMessage().contains("not serializable"));
        }
    }

    /**
     * It does have to render, though: anything that prints a request source mid-rewrite — a task description, for one — would
     * otherwise fail on a request that is merely in flight.
     */
    public void testPlaceholder_rendersRatherThanThrowing() throws IOException {
        SearchSourceBuilder source = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        FusedRescoreScope scope = FusedRescoreScope.install(source);
        QueryBuilder placeholder = confinedQueryOf(source).filter().get(0);

        assertTrue(render(placeholder).contains("\"hybrid_fused_window\""));
        assertTrue(render(placeholder).contains("\"resolved\":false"));
        scope.resolve(fusedOver("1"));
        assertTrue(render(placeholder).contains("\"resolved\":true"));
        assertNotNull("and the whole source still renders", source.toString());
    }

    /**
     * Two placeholders are interchangeable only if they are the same object — each one is a single install site in a single
     * request. Pinned because {@code SearchSourceBuilder#rewrite} asserts a source equals its own shallow copy, so what
     * {@code equals} means here is load-bearing under assertions.
     */
    public void testPlaceholder_equalsIsIdentity() {
        SearchSourceBuilder first = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        SearchSourceBuilder second = sourceWithRescorer(new QueryRescorerBuilder(new MatchQueryBuilder("text", "hot")));
        FusedRescoreScope.install(first);
        FusedRescoreScope.install(second);

        QueryBuilder one = confinedQueryOf(first).filter().get(0);
        QueryBuilder other = confinedQueryOf(second).filter().get(0);
        assertEquals(one, one);
        assertNotEquals(one, other);
        assertEquals("and stable, since it is consulted inside core's own assertions", one.hashCode(), one.hashCode());
    }

    // ---- harness ----

    private SearchSourceBuilder sourceWithRescorer(RescorerBuilder<?> rescorer) {
        return new SearchSourceBuilder().trackTotalHits(false).addRescorer(rescorer);
    }

    /** A fused window over one index, which is all any of these tests needs from a fusion result. */
    private HybridFusionQueryBuilder fusedOver(String... ids) {
        String[] indices = new String[ids.length];
        float[] scores = new float[ids.length];
        for (int i = 0; i < ids.length; i++) {
            indices[i] = INDEX;
            scores[i] = 1.0f / (i + 1);
        }
        return new HybridFusionQueryBuilder(ids, indices, scores, List.of());
    }

    /**
     * Core's own rewrite loop, from {@code Rewriteable#rewriteAndFetch}: keep rewriting while the source's identity changes.
     * Used instead of a single pass because the placeholder resolving is itself an identity change, so the source that is
     * dispatched is the one this converges on.
     */
    private SearchSourceBuilder coreRewriteToFixedPoint(SearchSourceBuilder source) throws IOException {
        QueryRewriteContext context = rewriteContext();
        SearchSourceBuilder current = source;
        for (SearchSourceBuilder next = current.rewrite(context); next != current; next = current.rewrite(context)) {
            current = next;
        }
        return current;
    }

    private QueryRescorerBuilder rescorerOf(SearchSourceBuilder source) {
        return rescorerOf(source, 0);
    }

    /**
     * The {@code index}-th of the user's rescorers as it was installed. After {@code install} the request carries ONE
     * element — the {@link FusedWindowGuardRescorerBuilder} — holding the window-confined chain in the user's order, so a
     * test that wants the user's rescorer has to go through the guard. See {@code FusedWindowGuardRescorer} for why the
     * chain collapses to one element rather than being wrapped element by element.
     */
    private QueryRescorerBuilder rescorerOf(SearchSourceBuilder source, int index) {
        assertEquals("install leaves exactly one rescorer, the guard", 1, source.rescores().size());
        FusedWindowGuardRescorerBuilder guard = (FusedWindowGuardRescorerBuilder) source.rescores().get(0);
        return guard.delegates().get(index);
    }

    private BoolQueryBuilder confinedQueryOf(SearchSourceBuilder source) {
        return (BoolQueryBuilder) rescorerOf(source).getRescoreQuery();
    }

    private String render(QueryBuilder query) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        query.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.toString();
    }

    /** Asserts a clause addresses exactly these ids inside exactly this index. */
    private void assertAddressedTo(QueryBuilder clause, String index, String... ids) {
        assertTrue("expected an _index-qualified bool, got " + clause, clause instanceof BoolQueryBuilder);
        BoolQueryBuilder qualified = (BoolQueryBuilder) clause;
        assertEquals("qualified by _id AND _index", 2, qualified.filter().size());
        assertEquals(Set.of(ids), ((IdsQueryBuilder) qualified.filter().get(0)).ids());
        TermQueryBuilder indexTerm = (TermQueryBuilder) qualified.filter().get(1);
        assertEquals("_index", indexTerm.fieldName());
        assertEquals(index, indexTerm.value());
    }

    /**
     * The bare minimum of a coordinator rewrite context: no async actions, no registries used by anything here. Deliberately
     * not a mock — these tests are about what core's real {@code rewrite} does with the builders, so the only thing worth
     * faking is the context it needs to be handed.
     */
    private QueryRewriteContext rewriteContext() {
        return new QueryRewriteContext() {
            @Override
            public NamedXContentRegistry getXContentRegistry() {
                return NamedXContentRegistry.EMPTY;
            }

            @Override
            public long nowInMillis() {
                return 0L;
            }

            @Override
            public NamedWriteableRegistry getWriteableRegistry() {
                return null;
            }

            @Override
            public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
                throw new UnsupportedOperationException("no async action is expected from a rescore query");
            }

            @Override
            public boolean hasAsyncActions() {
                return false;
            }

            @Override
            public void executeAsyncActions(@SuppressWarnings("rawtypes") ActionListener listener) {
                throw new UnsupportedOperationException("no async action is expected from a rescore query");
            }

            @Override
            public boolean validate() {
                return false;
            }
        };
    }

    /**
     * A rescore query that rewrites exactly once, which is what makes core replace the whole rescore list. {@code wrapper},
     * {@code neural}, {@code neural_sparse} and a {@code terms} lookup all do this against a real cluster; this is the same
     * event with no dependencies, so the identity bookkeeping can be asserted in a unit test.
     */
    private static final class RewritesOnceQueryBuilder extends AbstractQueryBuilder<RewritesOnceQueryBuilder> {

        private final QueryBuilder rewritesTo = new MatchQueryBuilder("text", "rewritten");

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            return rewritesTo;
        }

        @Override
        public String getWriteableName() {
            return "rewrites_once";
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getWriteableName()).endObject();
        }

        @Override
        protected org.apache.lucene.search.Query doToQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("rewritten before it can be executed");
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

    /** A rescorer that is not core's {@code query} rescorer — the shape a plugin can register and fused mode must refuse. */
    private static class UnsupportedRescorerBuilder extends RescorerBuilder<UnsupportedRescorerBuilder> {

        @Override
        public String getWriteableName() {
            return "not_a_query_rescorer";
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, ToXContent.Params params) {}

        @Override
        protected RescoreContext innerBuildContext(int windowSize, QueryShardContext context) {
            return null;
        }

        @Override
        public RescorerBuilder<UnsupportedRescorerBuilder> rewrite(QueryRewriteContext ctx) {
            return this;
        }
    }
}
