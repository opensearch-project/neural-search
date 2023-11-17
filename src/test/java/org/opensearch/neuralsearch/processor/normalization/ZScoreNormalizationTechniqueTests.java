/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class ZScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;

    /**
     * Z score will check the relative distance from the center of distribution in units of standard deviation
     * and hence can also be negative. It is using the formula of (score - mean_score)/std
     * When only two values are available their z-score numbers will be 1 and -1 correspondingly.
     * For more information regarding z-score you can check this link
     * https://www.z-table.com/
     *
     */
    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        // since we only have two scores of 0.5 and 0.2 their z-score numbers will be 1 and -1
        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, -1.0f) })
            )
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        assertCompoundTopDocs(
            new TopDocs(expectedCompoundDocs.getTotalHits(), expectedCompoundDocs.getScoreDocs().toArray(new ScoreDoc[0])),
            compoundTopDocs.get(0).getTopDocs().get(0)
        );
    }

    /**
     * Z score will check the relative distance from the center of distribution in units of standard deviation
     * and hence can also be negative. It is using the formula of (score - mean_score)/std
     * When only two values are available their z-score numbers will be 1 and -1 correspondingly as we see in the first query that returns only two document scores.
     * When we have more than two documents scores as in the second query the distribution will not be binary and will have different results based on where the center of gravity of the distribution is.
     * For more information regarding z-score you can check this link
     * https://www.z-table.com/
     *
     */
    public void testNormalization_whenResultFromOneShardMultipleSubQueries_thenSuccessful() {
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    // Calculated based on the formula (score - mean_score)/std
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, -1.0f) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    // Calculated based on the formula (score - mean_score)/std for the values of mean_score = (0.9 + 0.7 + 0.1)/3 ~ 0.56,
                    // std = sqrt(((0.9 - 0.56)^2 + (0.7 - 0.56)^2 + (0.1 - 0.56)^2)/3)
                    new ScoreDoc[] { new ScoreDoc(3, 0.98058068f), new ScoreDoc(4, 0.39223227f), new ScoreDoc(2, -1.37281295f) }
                )
            )
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        for (int i = 0; i < expectedCompoundDocs.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocs.getTopDocs().get(i), compoundTopDocs.get(0).getTopDocs().get(i));
        }
    }

    public void testNormalization_whenResultFromMultipleShardsMultipleSubQueries_thenSuccessful() {
        ZScoreNormalizationTechnique normalizationTechnique = new ZScoreNormalizationTechnique();
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.5f), new ScoreDoc(4, 0.2f) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 0.9f), new ScoreDoc(4, 0.7f), new ScoreDoc(2, 0.1f) }
                    )
                )
            ),
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(7, 2.9f), new ScoreDoc(9, 0.7f) }
                    )
                )
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocsShard1 = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, 1.0f), new ScoreDoc(4, -1.0f) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, 0.98058068f), new ScoreDoc(4, 0.39223227f), new ScoreDoc(2, -1.37281295f) }
                )
            )
        );

        CompoundTopDocs expectedCompoundDocsShard2 = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(7, 1.0f), new ScoreDoc(9, -1.0f) })
            )
        );

        assertNotNull(compoundTopDocs);
        assertEquals(2, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard1.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocsShard1.getTopDocs().get(i), compoundTopDocs.get(0).getTopDocs().get(i));
        }
        assertNotNull(compoundTopDocs.get(1).getTopDocs());
        for (int i = 0; i < expectedCompoundDocsShard2.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocsShard2.getTopDocs().get(i), compoundTopDocs.get(1).getTopDocs().get(i));
        }
    }

    private void assertCompoundTopDocs(TopDocs expected, TopDocs actual) {
        assertEquals(expected.totalHits.value, actual.totalHits.value);
        assertEquals(expected.totalHits.relation, actual.totalHits.relation);
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, DELTA_FOR_ASSERTION);
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].shardIndex, actual.scoreDocs[i].shardIndex);
        }
    }
}
