/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.test.OpenSearchTestCase;

public class ScoreCombinerTests extends OpenSearchTestCase {

    public void testEmptyResults_whenEmptyResultsAndDefaultMethod_thenNoProcessing() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        final CompoundTopDocs[] queryTopDocs = new CompoundTopDocs[0];
        List<Float> maxScores = scoreCombiner.combineScores(queryTopDocs, ScoreCombinationTechnique.DEFAULT);
        assertNotNull(maxScores);
        assertEquals(0, maxScores.size());
    }

    public void testCombination_whenMultipleSubqueriesResultsAndDefaultMethod_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final CompoundTopDocs[] queryTopDocs = new CompoundTopDocs[] {
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
            ) };

        TopDocsAndMaxScore[] topDocsAndMaxScore = new TopDocsAndMaxScore[] {
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                ),
                1.0f
            ),
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                ),
                0.9f
            ),
            new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), 0.0f) };
        List<Float> combinedMaxScores = scoreCombiner.combineScores(queryTopDocs, ScoreCombinationTechnique.DEFAULT);

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.length);

        assertEquals(3, queryTopDocs[0].scoreDocs.length);
        assertEquals(1.0, queryTopDocs[0].scoreDocs[0].score, 0.001f);
        assertEquals(1, queryTopDocs[0].scoreDocs[0].doc);
        assertEquals(1.0, queryTopDocs[0].scoreDocs[1].score, 0.001f);
        assertEquals(3, queryTopDocs[0].scoreDocs[1].doc);
        assertEquals(0.25, queryTopDocs[0].scoreDocs[2].score, 0.001f);
        assertEquals(2, queryTopDocs[0].scoreDocs[2].doc);

        assertEquals(4, queryTopDocs[1].scoreDocs.length);
        assertEquals(0.9, queryTopDocs[1].scoreDocs[0].score, 0.001f);
        assertEquals(2, queryTopDocs[1].scoreDocs[0].doc);
        assertEquals(0.6, queryTopDocs[1].scoreDocs[1].score, 0.001f);
        assertEquals(4, queryTopDocs[1].scoreDocs[1].doc);
        assertEquals(0.5, queryTopDocs[1].scoreDocs[2].score, 0.001f);
        assertEquals(7, queryTopDocs[1].scoreDocs[2].doc);
        assertEquals(0.01, queryTopDocs[1].scoreDocs[3].score, 0.001f);
        assertEquals(9, queryTopDocs[1].scoreDocs[3].doc);

        assertEquals(0, queryTopDocs[2].scoreDocs.length);

        assertEquals(3, combinedMaxScores.size());
        assertEquals(1.0, combinedMaxScores.get(0), 0.001f);
        assertEquals(0.9, combinedMaxScores.get(1), 0.001f);
        assertEquals(0.0, combinedMaxScores.get(2), 0.001f);
    }
}
