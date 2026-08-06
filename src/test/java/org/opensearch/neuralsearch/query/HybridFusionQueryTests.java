/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.Mockito.mock;

import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

public class HybridFusionQueryTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    public void testWriteableName() {
        HybridFusionQuery query = new HybridFusionQuery(new String[] { "d1" }, new float[] { 1.0f }, List.of());
        assertEquals("hybrid_fusion", query.getWriteableName());
    }

    public void testNotParseableFromXContent() {
        // Internal query built by the coordinator self-erase; never parsed from a request.
        expectThrows(UnsupportedOperationException.class, () -> HybridFusionQuery.fromXContent(null));
    }

    public void testSerializationRoundTrip() throws Exception {
        HybridFusionQuery original = new HybridFusionQuery(
            new String[] { "d1", "d2" },
            new float[] { 0.9f, 0.4f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana"))
        );
        HybridFusionQuery deserialized = copyWriteable(original, namedWriteableRegistry(), HybridFusionQuery::new);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    public void testSelfErasedShape_whenSourceQueriesPresent_thenTopPlusTail() {
        HybridFusionQuery query = new HybridFusionQuery(
            new String[] { "d1", "d2" },
            new float[] { 0.9f, 0.4f },
            List.of(new MatchQueryBuilder("title", "apple"), new MatchQueryBuilder("body", "banana"))
        );
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        // Top: one scoring SHOULD (constant_score) per ranked id; Tail: exactly one non-scoring FILTER clause.
        assertEquals(2, self.should().size());
        assertEquals(1, self.filter().size());
        assertTrue(self.should().get(0) instanceof ConstantScoreQueryBuilder);
        // The Tail is a bool{ should: [ real legs ] } — the leg union as one filter clause.
        assertTrue(self.filter().get(0) instanceof BoolQueryBuilder);
        assertEquals(2, ((BoolQueryBuilder) self.filter().get(0)).should().size());
    }

    public void testSelfErasedShape_whenNoSourceQueries_thenTopOnly() {
        HybridFusionQuery query = new HybridFusionQuery(new String[] { "d1", "d2" }, new float[] { 0.9f, 0.4f }, List.of());
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        assertEquals(2, self.should().size());
        assertEquals("Top-only fused query carries no Tail filter", 0, self.filter().size());
    }

    public void testSelfErasedShape_whenEmptyWindow_thenEmptyBool() {
        // An empty fused window produces an empty bool (no should, no filter) → compiles to match-no-docs.
        HybridFusionQuery query = new HybridFusionQuery(new String[0], new float[0], List.of());
        BoolQueryBuilder self = query.buildSelfErasedQuery();
        assertEquals(0, self.should().size());
        assertEquals(0, self.filter().size());
    }

    public void testDoXContent_isInformationalOnly() throws Exception {
        HybridFusionQuery query = new HybridFusionQuery(new String[] { "d1", "d2", "d3" }, new float[] { 0.9f, 0.5f, 0.1f }, List.of());
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
        query.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json.contains("hybrid_fusion"));
        assertTrue("informational representation reports the fused doc count", json.contains("fused_docs_count"));
        assertTrue(json.contains("3"));
    }

    public void testDoRewrite_whenNoSourceQueryChanges_thenReturnsSame() throws Exception {
        // Tail source queries that don't rewrite (already terminal term queries) → doRewrite returns the same instance.
        HybridFusionQuery query = new HybridFusionQuery(
            new String[] { "d1" },
            new float[] { 0.7f },
            List.of(new org.opensearch.index.query.TermQueryBuilder("text", "keyword"))
        );
        org.opensearch.index.query.QueryRewriteContext ctx = mock(org.opensearch.index.query.QueryRewriteContext.class);
        assertSame(query, query.rewrite(ctx));
    }

    public void testDoRewrite_whenSourceQueryRewrites_thenReturnsRewrittenCopy() throws Exception {
        // A source query that always rewrites to a new instance forces the changed==true branch: doRewrite returns a
        // NEW HybridFusionQuery preserving ids/scores/boost/queryName.
        org.opensearch.index.query.QueryBuilder alwaysRewrites = new org.opensearch.index.query.MatchAllQueryBuilder() {
            @Override
            protected org.opensearch.index.query.QueryBuilder doRewrite(org.opensearch.index.query.QueryRewriteContext c) {
                return new org.opensearch.index.query.MatchAllQueryBuilder(); // different instance each rewrite
            }
        };
        HybridFusionQuery query = new HybridFusionQuery(new String[] { "d1", "d2" }, new float[] { 0.7f, 0.3f }, List.of(alwaysRewrites));
        query.boost(1.0f);
        org.opensearch.index.query.QueryRewriteContext ctx = mock(org.opensearch.index.query.QueryRewriteContext.class);

        org.opensearch.index.query.QueryBuilder rewritten = query.rewrite(ctx);
        assertTrue(rewritten instanceof HybridFusionQuery);
        assertNotSame("changed source → new copy", query, rewritten);
        assertEquals(2, ((HybridFusionQuery) rewritten).buildSelfErasedQuery().should().size());
    }

    public void testExtractInnerHitBuilders_recursesIntoSourceQueries() {
        // A nested source query declaring inner_hits must be surfaced through extractInnerHitBuilders so the self-erased
        // query still fetches leg-level inner_hits.
        org.opensearch.index.query.NestedQueryBuilder nested = new org.opensearch.index.query.NestedQueryBuilder(
            "user",
            new org.opensearch.index.query.MatchQueryBuilder("user.name", "alice"),
            org.apache.lucene.search.join.ScoreMode.None
        ).innerHit(new org.opensearch.index.query.InnerHitBuilder());
        HybridFusionQuery query = new HybridFusionQuery(new String[] { "d1" }, new float[] { 0.9f }, List.of(nested));

        java.util.Map<String, org.opensearch.index.query.InnerHitContextBuilder> innerHits = new java.util.HashMap<>();
        query.extractInnerHitBuilders(innerHits);
        assertFalse("leg inner_hits must be surfaced", innerHits.isEmpty());
    }
}
