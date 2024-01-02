/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
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

        assertEquals(3, queryTopDocs.get(0).getScoreDocs().size());
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, queryTopDocs.get(0).getScoreDocs().get(0).doc);
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, queryTopDocs.get(0).getScoreDocs().get(1).doc);
        assertEquals(0.125, queryTopDocs.get(0).getScoreDocs().get(2).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, queryTopDocs.get(0).getScoreDocs().get(2).doc);

        assertEquals(4, queryTopDocs.get(1).getScoreDocs().size());
        assertEquals(0.45, queryTopDocs.get(1).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, queryTopDocs.get(1).getScoreDocs().get(0).doc);
        assertEquals(0.3, queryTopDocs.get(1).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(4, queryTopDocs.get(1).getScoreDocs().get(1).doc);
        assertEquals(0.25, queryTopDocs.get(1).getScoreDocs().get(2).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(7, queryTopDocs.get(1).getScoreDocs().get(2).doc);
        assertEquals(0.005, queryTopDocs.get(1).getScoreDocs().get(3).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(9, queryTopDocs.get(1).getScoreDocs().get(3).doc);

        assertEquals(0, queryTopDocs.get(2).getScoreDocs().size());
    }
}
