/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Collections;

import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

import java.util.List;

import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.test.OpenSearchTestCase;

public class ScoreCombinationTechniqueTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testEmptyResults_whenEmptyResultsAndDefaultMethod_thenNoProcessing() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(List.of())
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .build()
        );
    }

    public void testCombination_whenMultipleSubqueriesResultsAndDefaultMethod_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.001f) }
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
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );

        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .build()
        );

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(5, queryTopDocs.get(0).getScoreDocs().size());
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

    public void testCombination_whenMultipleSubqueriesWithSortingEnabled_thenScoresNull() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        // Create sort fields for documents
        Object[] sortFields1 = new Object[] { new BytesRef("value1") };
        Object[] sortFields2 = new Object[] { new BytesRef("value2") };
        Object[] sortFields3 = new Object[] { new BytesRef("value3") };
        Object[] sortFields4 = new Object[] { new BytesRef("value4") };
        // Define the sort field
        SortField[] sortFields = new SortField[] { new SortField("_id", SortField.Type.STRING, true) };
        Sort sort = new Sort(sortFields);

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(1, 0.5f, sortFields1),
                            new FieldDoc(2, 0.3f, sortFields2),
                            new FieldDoc(4, 0.1f, sortFields3) },
                        sortFields
                    ),
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] { new FieldDoc(3, 0.2f, sortFields2), new FieldDoc(5, Float.NaN, sortFields4) },
                        sortFields
                    )
                ),
                true,  // isSortEnabled set to true
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new FieldDoc[0], sortFields),
                    new TopFieldDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(2, 0.5f, sortFields1),
                            new FieldDoc(4, 1.0f, sortFields2),
                            new FieldDoc(7, 0.3f, sortFields3),
                            new FieldDoc(9, 0.2f, sortFields4) },
                        sortFields
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );

        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(true)
                .build()
        );

        // Verify results
        assertNotNull(queryTopDocs);
        assertEquals(2, queryTopDocs.size());

        // First CompoundTopDocs assertions
        assertEquals(5, queryTopDocs.get(0).getScoreDocs().size());
        assertTrue(queryTopDocs.get(0).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc firstDoc = queryTopDocs.get(0).getScoreDocs().get(0);
        assertEquals(Float.NaN, firstDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, firstDoc.doc);

        // Second CompoundTopDocs assertions
        assertEquals(4, queryTopDocs.get(1).getScoreDocs().size());
        assertTrue(queryTopDocs.get(1).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc secondDoc = queryTopDocs.get(1).getScoreDocs().get(0);
        assertEquals(Float.NaN, secondDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, secondDoc.doc);

        // Verify sort fields are preserved
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            for (TopDocs topDocs : compoundTopDocs.getTopDocs()) {
                if (topDocs instanceof TopFieldDocs) {
                    TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;
                    assertArrayEquals(sortFields, topFieldDocs.fields);
                } else {
                    fail(
                        "Expected TopFieldDocs but found "
                            + topDocs.getClass().getSimpleName()
                            + ". All TopDocs should be TopFieldDocs when sorting is enabled"
                    );
                }
            }
        }
    }

    public void testCombination_whenScorePrimaryWithTiebreakField_thenTiebreakPreservedAndScoreSlotSwapped() {
        // Multi-key [_score desc, price asc] sort (the collapse [_score, field] shape). isSortByScore is true, so
        // ScoreCombiner must swap ONLY the score slot (fields[0]) for the combined/normalized score and PRESERVE the
        // trailing tiebreaker slot(s). The pre-fix code did `new Object[]{ normalizedScore }`, which dropped the
        // tiebreaker (length -> 1); this pins the clone/swap branch that keeps it.
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        // fields[0] is a sentinel (not the real score) so a successful swap is unambiguous; fields[1] is the tiebreak.
        Object[] fieldsDoc1 = new Object[] { 111.0f, 100 };
        Object[] fieldsDoc2 = new Object[] { 111.0f, 200 };
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Sort sort = new Sort(sortFields);

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] { new FieldDoc(1, 0.9f, fieldsDoc1), new FieldDoc(2, 0.7f, fieldsDoc2) },
                        sortFields
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );

        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(true)
                .build()
        );

        // The swapped sort fields land on the COMBINED docs exposed via getScoreDocs() (getTopDocs() still holds the
        // per-sub-query originals with the sentinel). getScoreDoc() builds each combined FieldDoc from docIdSortFieldMap.
        List<ScoreDoc> combinedDocs = queryTopDocs.getFirst().getScoreDocs();
        assertEquals("both docs survive combination", 2, combinedDocs.size());
        for (ScoreDoc scoreDoc : combinedDocs) {
            FieldDoc fieldDoc = (FieldDoc) scoreDoc;
            // tiebreaker slot preserved => length stays 2 (pre-fix code collapsed it to a single [score] element)
            assertEquals("trailing tiebreaker slot must be preserved", 2, fieldDoc.fields.length);
            // slot 0 swapped to the combined score (== the doc's score), no longer the 111f sentinel
            assertEquals(
                "score slot must hold the combined score, not the sentinel",
                fieldDoc.score,
                ((Number) fieldDoc.fields[0]).floatValue(),
                DELTA_FOR_SCORE_ASSERTION
            );
            // slot 1 is the original tiebreaker, untouched
            if (fieldDoc.doc == 1) {
                assertEquals("tiebreaker preserved for doc1", 100, ((Number) fieldDoc.fields[1]).intValue());
            } else if (fieldDoc.doc == 2) {
                assertEquals("tiebreaker preserved for doc2", 200, ((Number) fieldDoc.fields[1]).intValue());
            }
        }
    }

    public void testCombination_whenSortEnabledAndShardHasNoResults_thenNoException() {
        // Reproduces https://github.com/opensearch-project/neural-search/issues/1934:
        // with sort enabled (as search_after requires), a shard that contributed no results past the cursor
        // arrives at the coordinator with an empty top docs list but a non-zero total hits count (see
        // CompoundTopDocs constructor, which preserves totalHits while initializing an empty list when
        // scoreDocs.length < 2). Before the fix, getDocIdSortFieldsMap probed topFieldDocs.getFirst()
        // unconditionally and threw a bare NoSuchElementException on that empty list.
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        Object[] sortFields1 = new Object[] { new BytesRef("value1") };
        Object[] sortFields2 = new Object[] { new BytesRef("value2") };
        SortField[] sortFields = new SortField[] { new SortField("_id", SortField.Type.STRING, true) };
        Sort sort = new Sort(sortFields);

        // First shard has results, second shard is exhausted: empty top docs list but total hits > 0.
        CompoundTopDocs shardWithResults = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopFieldDocs(
                    new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                    new FieldDoc[] { new FieldDoc(1, 0.5f, sortFields1), new FieldDoc(2, 0.3f, sortFields2) },
                    sortFields
                )
            ),
            true,
            SEARCH_SHARD
        );
        CompoundTopDocs exhaustedShard = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            Collections.emptyList(),
            true,
            SEARCH_SHARD
        );
        final List<CompoundTopDocs> queryTopDocs = List.of(shardWithResults, exhaustedShard);

        // Must not throw NoSuchElementException.
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(false)
                .build()
        );

        // The shard with results is combined as usual.
        assertEquals(2, queryTopDocs.get(0).getScoreDocs().size());
        // The exhausted shard yields no score docs but keeps its total hit count (search_after semantics:
        // total_hits is pagination-independent, so a shard past its cursor still reports its full match count).
        assertTrue(queryTopDocs.get(1).getScoreDocs().isEmpty());
        assertEquals(3, queryTopDocs.get(1).getTotalHits().value());
    }

    public void testCombination_whenMultipleSubqueriesResultsWithMinScore_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.001f) }
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
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );

        Float minScore = 0.2f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .minScore(minScore)
                .build()
        );

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(2, queryTopDocs.get(0).getScoreDocs().size());
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, queryTopDocs.get(0).getScoreDocs().get(0).doc);
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, queryTopDocs.get(0).getScoreDocs().get(1).doc);

        assertEquals(3, queryTopDocs.get(1).getScoreDocs().size());
        assertEquals(0.45, queryTopDocs.get(1).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, queryTopDocs.get(1).getScoreDocs().get(0).doc);
        assertEquals(0.3, queryTopDocs.get(1).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(4, queryTopDocs.get(1).getScoreDocs().get(1).doc);
        assertEquals(0.25, queryTopDocs.get(1).getScoreDocs().get(2).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(7, queryTopDocs.get(1).getScoreDocs().get(2).doc);

        assertEquals(0, queryTopDocs.get(2).getScoreDocs().size());
    }

    public void testCombination_whenMultipleSubqueriesResultsWithZeroMinScore_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.001f) }
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
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );

        Float minScore = 0.0f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .minScore(minScore)
                .build()
        );

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(5, queryTopDocs.get(0).getScoreDocs().size());
        assertEquals(TotalHits.Relation.EQUAL_TO, queryTopDocs.get(0).getTotalHits().relation());
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, queryTopDocs.get(0).getScoreDocs().get(0).doc);
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, queryTopDocs.get(0).getScoreDocs().get(1).doc);
        assertEquals(.125, queryTopDocs.get(0).getScoreDocs().get(2).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, queryTopDocs.get(0).getScoreDocs().get(2).doc);

        assertEquals(4, queryTopDocs.get(1).getScoreDocs().size());
        assertEquals(TotalHits.Relation.EQUAL_TO, queryTopDocs.get(1).getTotalHits().relation());
        assertEquals(0.45, queryTopDocs.get(1).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, queryTopDocs.get(1).getScoreDocs().get(0).doc);
        assertEquals(0.3, queryTopDocs.get(1).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(4, queryTopDocs.get(1).getScoreDocs().get(1).doc);
        assertEquals(0.25, queryTopDocs.get(1).getScoreDocs().get(2).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(7, queryTopDocs.get(1).getScoreDocs().get(2).doc);
        assertEquals(.005, queryTopDocs.get(1).getScoreDocs().get(3).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(9, queryTopDocs.get(1).getScoreDocs().get(3).doc);

        assertEquals(0, queryTopDocs.get(2).getScoreDocs().size());
    }

    public void testCombination_whenMultipleSubqueriesResultsWithLargeMinScore_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.001f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.001f) }
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
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );

        Float minScore = 1000.2f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .minScore(minScore)
                .build()
        );

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(0, queryTopDocs.get(0).getScoreDocs().size());

        assertEquals(0, queryTopDocs.get(1).getScoreDocs().size());

        assertEquals(0, queryTopDocs.get(2).getScoreDocs().size());
    }

    public void testCombination_whenMultipleSubqueriesResultsWithCloseMinScore_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, .25f), new ScoreDoc(4, 0.01f) }
                    ),
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(3, 1.0f), new ScoreDoc(5, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(2, 0.9f), new ScoreDoc(4, 0.6f), new ScoreDoc(7, 0.5f), new ScoreDoc(9, 0.01f) }
                    )
                ),
                false,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0])
                ),
                false,
                SEARCH_SHARD
            )
        );

        Float minScore = 0.0049999f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .minScore(minScore)
                .build()
        );

        assertNotNull(queryTopDocs);
        assertEquals(3, queryTopDocs.size());

        assertEquals(5, queryTopDocs.get(0).getScoreDocs().size());
        // Filtered size is equal to original size, and we get EQUAL_TO because the existing relation is EQUAL_TO
        assertEquals(TotalHits.Relation.EQUAL_TO, queryTopDocs.get(0).getTotalHits().relation());
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(0).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, queryTopDocs.get(0).getScoreDocs().get(0).doc);
        assertEquals(.5, queryTopDocs.get(0).getScoreDocs().get(1).score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, queryTopDocs.get(0).getScoreDocs().get(1).doc);

        assertEquals(4, queryTopDocs.get(1).getScoreDocs().size());
        // Filtered size is equal to original size, and we get GREATER_THAN_OR_EQUAL_TO because the existing relation is
        // GREATER_THAN_OR_EQUAL_TO
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, queryTopDocs.get(1).getTotalHits().relation());
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

    public void testCombination_whenMultipleSubqueriesResultsWithMinScoreAndSortingWithID_thenScoresNull() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        // Create sort fields for documents
        Object[] sortFields1 = new Object[] { new BytesRef("value1") };
        Object[] sortFields2 = new Object[] { new BytesRef("value2") };
        Object[] sortFields3 = new Object[] { new BytesRef("value3") };
        Object[] sortFields4 = new Object[] { new BytesRef("value4") };
        // Define the sort field
        SortField[] sortFields = new SortField[] { new SortField("_id", SortField.Type.STRING, true) };
        Sort sort = new Sort(sortFields);

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(1, 0.5f, sortFields1),
                            new FieldDoc(2, 0.3f, sortFields2),
                            new FieldDoc(4, 0.1f, sortFields3) },
                        sortFields
                    ),
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] { new FieldDoc(3, 0.2f, sortFields2), new FieldDoc(5, Float.NaN, sortFields4) },
                        sortFields
                    )
                ),
                true,  // isSortEnabled set to true
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new FieldDoc[0], sortFields),
                    new TopFieldDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(2, 0.5f, sortFields1),
                            new FieldDoc(4, 1.0f, sortFields2),
                            new FieldDoc(7, 0.3f, sortFields3),
                            new FieldDoc(9, 0.2f, sortFields4) },
                        sortFields
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );

        Float minScore = 0.2f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(true)
                .minScore(minScore)
                .build()
        );

        // Verify results
        assertNotNull(queryTopDocs);
        assertEquals(2, queryTopDocs.size());

        // First CompoundTopDocs assertions
        assertEquals(5, queryTopDocs.get(0).getScoreDocs().size());
        assertTrue(queryTopDocs.get(0).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc firstDoc = queryTopDocs.get(0).getScoreDocs().get(0);
        assertEquals(Float.NaN, firstDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3, firstDoc.doc);

        // Second CompoundTopDocs assertions
        assertEquals(4, queryTopDocs.get(1).getScoreDocs().size());
        assertTrue(queryTopDocs.get(1).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc secondDoc = queryTopDocs.get(1).getScoreDocs().get(0);
        assertEquals(Float.NaN, secondDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, secondDoc.doc);
        ScoreDoc thirdDoc = queryTopDocs.get(1).getScoreDocs().get(1);
        assertEquals(Float.NaN, thirdDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(4, thirdDoc.doc);

        // Verify sort fields are preserved
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            for (TopDocs topDocs : compoundTopDocs.getTopDocs()) {
                if (topDocs instanceof TopFieldDocs) {
                    TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;
                    assertArrayEquals(sortFields, topFieldDocs.fields);
                } else {
                    fail(
                        "Expected TopFieldDocs but found "
                            + topDocs.getClass().getSimpleName()
                            + ". All TopDocs should be TopFieldDocs when sorting is enabled"
                    );
                }
            }
        }
    }

    public void testCombination_whenMultipleSubqueriesResultsWithMinScoreAndSortingWithScore_thenScoresCombined() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();

        // Create sort fields for documents
        Object[] sortFields1 = new Object[] { new BytesRef("value1") };
        Object[] sortFields2 = new Object[] { new BytesRef("value2") };
        Object[] sortFields3 = new Object[] { new BytesRef("value3") };
        Object[] sortFields4 = new Object[] { new BytesRef("value4") };
        // Define the sort field
        SortField[] sortFields = new SortField[] { new SortField(null, SortField.Type.SCORE) };
        Sort sort = new Sort(sortFields);

        final List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(
                        new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(1, 0.5f, sortFields1),
                            new FieldDoc(2, 0.3f, sortFields2),
                            new FieldDoc(4, 0.1f, sortFields3) },
                        sortFields
                    ),
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] { new FieldDoc(3, 0.2f, sortFields2), new FieldDoc(5, 0.1f, sortFields4) },
                        sortFields
                    )
                ),
                true,  // isSortEnabled set to true
                SEARCH_SHARD
            ),
            new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new FieldDoc[0], sortFields),
                    new TopFieldDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(4, 1.0f, sortFields2),
                            new FieldDoc(2, 0.5f, sortFields1),
                            new FieldDoc(7, 0.3f, sortFields3),
                            new FieldDoc(9, 0.2f, sortFields4) },
                        sortFields
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );

        Float minScore = 0.2f;
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(true)
                .minScore(minScore)
                .build()
        );

        // Verify results
        assertNotNull(queryTopDocs);
        assertEquals(2, queryTopDocs.size());

        // First CompoundTopDocs assertions
        assertEquals(1, queryTopDocs.get(0).getScoreDocs().size());
        assertTrue(queryTopDocs.get(0).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc firstDoc = queryTopDocs.get(0).getScoreDocs().get(0);
        assertEquals(0.25, firstDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, firstDoc.doc);

        // Second CompoundTopDocs assertions
        assertEquals(2, queryTopDocs.get(1).getScoreDocs().size());
        assertTrue(queryTopDocs.get(1).getScoreDocs().get(0) instanceof ScoreDoc);
        ScoreDoc secondDoc = queryTopDocs.get(1).getScoreDocs().get(0);
        assertEquals(0.5, secondDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(4, secondDoc.doc);
        ScoreDoc thirdDoc = queryTopDocs.get(1).getScoreDocs().get(1);
        assertEquals(0.25, thirdDoc.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, thirdDoc.doc);

        // Verify sort fields are preserved
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            for (TopDocs topDocs : compoundTopDocs.getTopDocs()) {
                if (topDocs instanceof TopFieldDocs) {
                    TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;
                    assertArrayEquals(sortFields, topFieldDocs.fields);
                } else {
                    fail(
                        "Expected TopFieldDocs but found "
                            + topDocs.getClass().getSimpleName()
                            + ". All TopDocs should be TopFieldDocs when sorting is enabled"
                    );
                }
            }
        }
    }
}
