/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.test.OpenSearchTestCase;

public class ScoreCombinationTechniqueTests extends OpenSearchTestCase {

    public void testEmptyResults_whenEmptyResultsAndDefaultMethod_thenNoProcessing() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        scoreCombiner.combineScores(List.of(), ScoreCombinationFactory.DEFAULT_METHOD);
    }

    public void testCombination_whenMultipleSubqueriesResultsAndDefaultMethod_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.001f) }
                    )
                )
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                )
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                )
            )
        );

        scoreCombiner.combineScores(queryTopDocs, ScoreCombinationFactory.DEFAULT_METHOD);

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(3, queryTopDocs.get(0).scoreDocs.length);
        assertEquals(1.0, queryTopDocs.get(0).scoreDocs[0].score, 0.001f);
        assertEquals(1, queryTopDocs.get(0).scoreDocs[0].doc);
        assertEquals(1.0, queryTopDocs.get(0).scoreDocs[1].score, 0.001f);
        assertEquals(3, queryTopDocs.get(0).scoreDocs[1].doc);
        assertEquals(0.25, queryTopDocs.get(0).scoreDocs[2].score, 0.001f);
        assertEquals(2, queryTopDocs.get(0).scoreDocs[2].doc);

        assertEquals(4, queryTopDocs.get(1).scoreDocs.length);
        assertEquals(0.9, queryTopDocs.get(1).scoreDocs[0].score, 0.001f);
        assertEquals(2, queryTopDocs.get(1).scoreDocs[0].doc);
        assertEquals(0.6, queryTopDocs.get(1).scoreDocs[1].score, 0.001f);
        assertEquals(4, queryTopDocs.get(1).scoreDocs[1].doc);
        assertEquals(0.5, queryTopDocs.get(1).scoreDocs[2].score, 0.001f);
        assertEquals(7, queryTopDocs.get(1).scoreDocs[2].doc);
        assertEquals(0.01, queryTopDocs.get(1).scoreDocs[3].score, 0.001f);
        assertEquals(9, queryTopDocs.get(1).scoreDocs[3].doc);

        assertEquals(0, queryTopDocs.get(2).scoreDocs.length);
    }
}
