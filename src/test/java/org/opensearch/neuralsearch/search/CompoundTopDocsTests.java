/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class CompoundTopDocsTests extends OpenSearchQueryTestCase {

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
        List<TopDocs> topDocs = List.of(topDocs1, topDocs2);
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), topDocs);
        assertNotNull(compoundTopDocs);
        assertEquals(topDocs, compoundTopDocs.getCompoundTopDocs());
    }

    public void testBasics_whenCreateWithoutTopDocs_thenTopDocsIsNull() {
        CompoundTopDocs hybridQueryScoreTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, RandomUtils.nextFloat()),
                new ScoreDoc(4, RandomUtils.nextFloat()),
                new ScoreDoc(5, RandomUtils.nextFloat()) }
        );
        assertNotNull(hybridQueryScoreTopDocs);
        assertNull(hybridQueryScoreTopDocs.getCompoundTopDocs());
    }

    public void testBasics_whenMultipleTopDocsOfDifferentLength_thenReturnTopDocsWithMostHits() {
        TopDocs topDocs1 = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), null);
        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(2, RandomUtils.nextFloat()), new ScoreDoc(4, RandomUtils.nextFloat()) }
        );
        List<TopDocs> topDocs = List.of(topDocs1, topDocs2);
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), topDocs);
        assertNotNull(compoundTopDocs);
        assertNotNull(compoundTopDocs.scoreDocs);
        assertEquals(2, compoundTopDocs.scoreDocs.length);
    }

    public void testBasics_whenMultipleTopDocsIsNull_thenScoreDocsIsNull() {
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), (List<TopDocs>) null);
        assertNotNull(compoundTopDocs);
        assertNull(compoundTopDocs.scoreDocs);

        CompoundTopDocs compoundTopDocsWithNullArray = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(null, null)
        );
        assertNotNull(compoundTopDocsWithNullArray);
        assertNotNull(compoundTopDocsWithNullArray.scoreDocs);
        assertEquals(0, compoundTopDocsWithNullArray.scoreDocs.length);
    }
}
