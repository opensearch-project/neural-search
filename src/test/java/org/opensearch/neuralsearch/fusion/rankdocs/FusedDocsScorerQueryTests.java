/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.rankdocs;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class FusedDocsScorerQueryTests extends OpenSearchTestCase {

    public void testConstructor_rejectsNullArrays() {
        expectThrows(IllegalArgumentException.class, () -> new FusedDocsScorerQuery(null, new float[0]));
        expectThrows(IllegalArgumentException.class, () -> new FusedDocsScorerQuery(new int[0], null));
    }

    public void testConstructor_rejectsLengthMismatch() {
        expectThrows(IllegalArgumentException.class, () -> new FusedDocsScorerQuery(new int[] { 1, 2 }, new float[] { 0.5f }));
    }

    public void testCreateWeight_notImplementedYet() {
        // Skeleton: the docId/PIT fast-path scoring lands in a later PR; until then createWeight throws.
        FusedDocsScorerQuery query = new FusedDocsScorerQuery(new int[] { 1, 2 }, new float[] { 0.9f, 0.4f });
        expectThrows(UnsupportedOperationException.class, () -> query.createWeight(mock(IndexSearcher.class), ScoreMode.COMPLETE, 1.0f));
    }

    public void testToString_reportsFusedDocCount() {
        FusedDocsScorerQuery query = new FusedDocsScorerQuery(new int[] { 1, 2, 3 }, new float[] { 0.9f, 0.5f, 0.1f });
        assertTrue(query.toString("anyField").contains("fusedDocs=3"));
    }

    public void testEqualsAndHashCode() {
        FusedDocsScorerQuery a = new FusedDocsScorerQuery(new int[] { 1, 2 }, new float[] { 0.9f, 0.4f });
        FusedDocsScorerQuery b = new FusedDocsScorerQuery(new int[] { 1, 2 }, new float[] { 0.9f, 0.4f });
        FusedDocsScorerQuery c = new FusedDocsScorerQuery(new int[] { 1, 3 }, new float[] { 0.9f, 0.4f });
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testFusedDocRecordAccessors() {
        FusedDoc doc = new FusedDoc(7, 0.42f, 2);
        assertEquals(7, doc.docId());
        assertEquals(0.42f, doc.score(), 0.0f);
        assertEquals(2, doc.shardIndex());
    }

    public void testEquals_falseForNullAndOtherType() {
        FusedDocsScorerQuery query = new FusedDocsScorerQuery(new int[] { 1 }, new float[] { 0.5f });
        assertNotEquals(query, null);
        assertNotEquals(query, "not a query");
    }

    public void testVisit_visitsLeaf() {
        FusedDocsScorerQuery query = new FusedDocsScorerQuery(new int[] { 1, 2 }, new float[] { 0.9f, 0.4f });
        java.util.concurrent.atomic.AtomicInteger leafVisits = new java.util.concurrent.atomic.AtomicInteger();
        query.visit(new org.apache.lucene.search.QueryVisitor() {
            @Override
            public void visitLeaf(org.apache.lucene.search.Query q) {
                leafVisits.incrementAndGet();
            }
        });
        assertEquals(1, leafVisits.get());
    }
}
