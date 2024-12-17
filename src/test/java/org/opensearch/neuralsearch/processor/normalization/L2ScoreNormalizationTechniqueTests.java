/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

/**
 * Abstracts normalization of scores based on min-max method
 */
public class L2ScoreNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    private static final float DELTA_FOR_ASSERTION = 0.0001f;
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
        Float[] scores = { 0.5f, 0.2f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scores[0]), new ScoreDoc(4, scores[1]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(2, l2Norm(scores[0], Arrays.asList(scores))),
                        new ScoreDoc(4, l2Norm(scores[1], Arrays.asList(scores))) }
                )
            ),
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        assertCompoundTopDocs(
            new TopDocs(expectedCompoundDocs.getTotalHits(), expectedCompoundDocs.getScoreDocs().toArray(new ScoreDoc[0])),
            compoundTopDocs.get(0).getTopDocs().get(0)
        );
    }

    public void testNormalization_whenResultFromOneShardMultipleSubQueries_thenSuccessful() {
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
        Float[] scoresQuery1 = { 0.5f, 0.2f };
        Float[] scoresQuery2 = { 0.9f, 0.7f, 0.1f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scoresQuery1[0]), new ScoreDoc(4, scoresQuery1[1]) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresQuery2[0]),
                            new ScoreDoc(4, scoresQuery2[1]),
                            new ScoreDoc(2, scoresQuery2[2]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(2, l2Norm(scoresQuery1[0], Arrays.asList(scoresQuery1))),
                        new ScoreDoc(4, l2Norm(scoresQuery1[1], Arrays.asList(scoresQuery1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresQuery2[0], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(4, l2Norm(scoresQuery2[1], Arrays.asList(scoresQuery2))),
                        new ScoreDoc(2, l2Norm(scoresQuery2[2], Arrays.asList(scoresQuery2))) }
                )
            ),
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocs);
        assertEquals(1, compoundTopDocs.size());
        assertNotNull(compoundTopDocs.get(0).getTopDocs());
        for (int i = 0; i < expectedCompoundDocs.getTopDocs().size(); i++) {
            assertCompoundTopDocs(expectedCompoundDocs.getTopDocs().get(i), compoundTopDocs.get(0).getTopDocs().get(i));
        }
    }

    public void testNormalization_whenResultFromMultipleShardsMultipleSubQueries_thenSuccessful() {
        L2ScoreNormalizationTechnique normalizationTechnique = new L2ScoreNormalizationTechnique();
        Float[] scoresShard1Query1 = { 0.5f, 0.2f };
        Float[] scoresShard1and2Query3 = { 0.9f, 0.7f, 0.1f, 0.8f, 0.7f, 0.6f, 0.5f };
        Float[] scoresShard2Query2 = { 2.9f, 0.7f };
        List<CompoundTopDocs> compoundTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, scoresShard1Query1[0]), new ScoreDoc(4, scoresShard1Query1[1]) }
                    ),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresShard1and2Query3[0]),
                            new ScoreDoc(4, scoresShard1and2Query3[1]),
                            new ScoreDoc(2, scoresShard1and2Query3[2]) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(7, scoresShard2Query2[0]), new ScoreDoc(9, scoresShard2Query2[1]) }
                    ),
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(3, scoresShard1and2Query3[3]),
                            new ScoreDoc(9, scoresShard1and2Query3[4]),
                            new ScoreDoc(10, scoresShard1and2Query3[5]),
                            new ScoreDoc(15, scoresShard1and2Query3[6]) }
                    )
                ),
                false,
                SEARCH_SHARD
            )
        );
        normalizationTechnique.normalize(compoundTopDocs);

        CompoundTopDocs expectedCompoundDocsShard1 = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(2, l2Norm(scoresShard1Query1[0], Arrays.asList(scoresShard1Query1))),
                        new ScoreDoc(4, l2Norm(scoresShard1Query1[1], Arrays.asList(scoresShard1Query1))) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresShard1and2Query3[0], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(4, l2Norm(scoresShard1and2Query3[1], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(2, l2Norm(scoresShard1and2Query3[2], Arrays.asList(scoresShard1and2Query3))) }
                )
            ),
            false,
            SEARCH_SHARD
        );

        CompoundTopDocs expectedCompoundDocsShard2 = new CompoundTopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(7, l2Norm(scoresShard2Query2[0], Arrays.asList(scoresShard2Query2))),
                        new ScoreDoc(9, l2Norm(scoresShard2Query2[1], Arrays.asList(scoresShard2Query2))) }
                ),
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, l2Norm(scoresShard1and2Query3[3], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(9, l2Norm(scoresShard1and2Query3[4], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(10, l2Norm(scoresShard1and2Query3[5], Arrays.asList(scoresShard1and2Query3))),
                        new ScoreDoc(15, l2Norm(scoresShard1and2Query3[6], Arrays.asList(scoresShard1and2Query3))) }
                )
            ),
            false,
            SEARCH_SHARD
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

    private float l2Norm(float score, List<Float> scores) {
        return score / (float) Math.sqrt(scores.stream().map(Float::doubleValue).map(s -> s * s).mapToDouble(Double::doubleValue).sum());
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
