/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HybridQueryTopdDocsTests extends OpenSearchQueryTestCase {

    public void testBasics_whenCreateWithTopDocsArray_thenSuccessful() {
        TopDocs topDocs1 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(0, RandomUtils.nextFloat()), new ScoreDoc(1, RandomUtils.nextFloat()) }
        );
        TopDocs topDocs2 = new TopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, RandomUtils.nextFloat()),
                new ScoreDoc(4, RandomUtils.nextFloat()),
                new ScoreDoc(5, RandomUtils.nextFloat()) }
        );
        TopDocs[] topDocs = new TopDocs[] { topDocs1, topDocs2 };
        HybridQueryTopDocs hybridQueryTopDocs = new HybridQueryTopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), topDocs);
        assertNotNull(hybridQueryTopDocs);
        assertEquals(topDocs, hybridQueryTopDocs.getHybridQueryTopdDocs());
    }

    public void testBasics_whenCreateWithoutTopDocs_thenTopDocsIsNull() {
        HybridQueryTopDocs hybridQueryScoreTopDocs = new HybridQueryTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, RandomUtils.nextFloat()),
                new ScoreDoc(4, RandomUtils.nextFloat()),
                new ScoreDoc(5, RandomUtils.nextFloat()) }
        );
        assertNotNull(hybridQueryScoreTopDocs);
        assertNull(hybridQueryScoreTopDocs.getHybridQueryTopdDocs());
    }

    public void testBasics_whenFirstTopDocsIsNull_thenTopDocsIsNull() {
        TopDocs topDocs1 = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), null);
        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(2, RandomUtils.nextFloat()), new ScoreDoc(4, RandomUtils.nextFloat()) }
        );
        TopDocs[] topDocs = new TopDocs[] { topDocs1, topDocs2 };
        HybridQueryTopDocs hybridQueryTopDocs = new HybridQueryTopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), topDocs);
        assertNotNull(hybridQueryTopDocs);
        assertNull(hybridQueryTopDocs.scoreDocs);
    }
}
