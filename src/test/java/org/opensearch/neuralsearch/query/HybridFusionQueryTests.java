/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

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
}
