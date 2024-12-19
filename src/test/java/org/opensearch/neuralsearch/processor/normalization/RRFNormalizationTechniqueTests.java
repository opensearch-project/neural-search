/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstracts testing of normalization of scores based on RRF method
 */
public class RRFNormalizationTechniqueTests extends OpenSearchQueryTestCase {
    static final int RANK_CONSTANT = 60;
    private ScoreNormalizationUtil scoreNormalizationUtil = new ScoreNormalizationUtil();
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testDescribe() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        assertEquals("rrf, rank_constant 60", normalizationTechnique.describe());
    }

    public void testNormalization_whenResultFromOneShardOneSubQuery_thenSuccessful() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scores = { 0.5f, 0.2f };
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
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
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
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scoresQuery1 = { 0.5f, 0.2f };
        float[] scoresQuery2 = { 0.9f, 0.7f, 0.1f };
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
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)), new ScoreDoc(2, rrfNorm(2)) }
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
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        float[] scoresShard1Query1 = { 0.5f, 0.2f };
        float[] scoresShard1and2Query3 = { 0.9f, 0.7f, 0.1f, 0.8f, 0.7f, 0.6f, 0.5f };
        float[] scoresShard2Query2 = { 2.9f, 0.7f };
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
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(compoundTopDocs)
            .normalizationTechnique(normalizationTechnique)
            .build();
        normalizationTechnique.normalize(normalizeScoresDTO);

        CompoundTopDocs expectedCompoundDocsShard1 = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(2, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)) }
                ),
                new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] { new ScoreDoc(3, rrfNorm(0)), new ScoreDoc(4, rrfNorm(1)), new ScoreDoc(2, rrfNorm(2)) }
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
                    new ScoreDoc[] { new ScoreDoc(7, rrfNorm(0)), new ScoreDoc(9, rrfNorm(1)) }
                ),
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(3, rrfNorm(3)),
                        new ScoreDoc(9, rrfNorm(4)),
                        new ScoreDoc(10, rrfNorm(5)),
                        new ScoreDoc(15, rrfNorm(6)) }
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

    public void testExplainWithEmptyAndNullList() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        normalizationTechnique.explain(List.of());

        List<CompoundTopDocs> compoundTopDocs = new ArrayList<>();
        compoundTopDocs.add(null);
        normalizationTechnique.explain(compoundTopDocs);
    }

    public void testExplainWithSingleTopDocs() {
        RRFNormalizationTechnique normalizationTechnique = new RRFNormalizationTechnique(Map.of(), scoreNormalizationUtil);
        CompoundTopDocs topDocs = createCompoundTopDocs(new float[] { 0.8f }, 1);
        List<CompoundTopDocs> queryTopDocs = Collections.singletonList(topDocs);

        Map<DocIdAtSearchShard, ExplanationDetails> explanation = normalizationTechnique.explain(queryTopDocs);

        assertNotNull(explanation);
        assertEquals(1, explanation.size());
        assertTrue(explanation.containsKey(new DocIdAtSearchShard(0, new SearchShard("test_index", 0, "uuid"))));
    }

    private float rrfNorm(int rank) {
        // 1.0f / (float) (rank + RANK_CONSTANT + 1);
        return BigDecimal.ONE.divide(BigDecimal.valueOf(rank + RANK_CONSTANT + 1), 10, RoundingMode.HALF_UP).floatValue();
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

    private CompoundTopDocs createCompoundTopDocs(float[] scores, int size) {
        ScoreDoc[] scoreDocs = new ScoreDoc[size];
        for (int i = 0; i < size; i++) {
            scoreDocs[i] = new ScoreDoc(i, scores[i]);
        }
        TopDocs singleTopDocs = new TopDocs(new TotalHits(size, TotalHits.Relation.EQUAL_TO), scoreDocs);

        List<TopDocs> topDocsList = Collections.singletonList(singleTopDocs);
        TopDocs topDocs = new TopDocs(new TotalHits(size, TotalHits.Relation.EQUAL_TO), scoreDocs);
        SearchShard searchShard = new SearchShard("test_index", 0, "uuid");

        return new CompoundTopDocs(
            new TotalHits(size, TotalHits.Relation.EQUAL_TO),
            topDocsList,
            false, // isSortEnabled
            searchShard
        );
    }
}
