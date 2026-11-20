/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
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

    public void testCombination_whenScorePrimaryWithTiebreakField_thenOrderedByCombinedScoreThenTiebreaker() {
        // [_score desc, price asc] without collapse: the page is ordered by the COMBINED score, ties broken by the field,
        // not by the raw per-sub-query scores the shard sorted by. Scores below are the normalized per-sub-query scores the
        // combiner receives; a 0 in a sub-query means the document did not match it.
        // doc1: [1.0, 0 ] price 300 -> mean 0.5 doc2: [0.2, 1.0] price 100 -> mean 0.6
        // doc3: [0.5, 0.5] price 200 -> mean 0.5 doc4: [0 , 0.4] price 50 -> mean 0.2
        // combined order: doc2 (0.6), doc3 and doc1 tie at 0.5 -> price asc -> doc3 (200) before doc1 (300), doc4 (0.2).
        // A merge on the raw sort values would give doc2, doc1 (raw 1.0), doc3 (0.5), doc4.
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Sort sort = new Sort(sortFields);
        List<CompoundTopDocs> queryTopDocs = List.of(scorePrimaryShard(sortFields));

        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(false)
                .build()
        );

        List<ScoreDoc> combinedDocs = queryTopDocs.getFirst().getScoreDocs();
        assertEquals(List.of(2, 3, 1, 4), combinedDocs.stream().map(scoreDoc -> scoreDoc.doc).toList());
        float[] expectedCombinedScores = new float[] { 0.6f, 0.5f, 0.5f, 0.2f };
        int[] expectedPrices = new int[] { 100, 200, 300, 50 };
        for (int i = 0; i < combinedDocs.size(); i++) {
            FieldDoc fieldDoc = (FieldDoc) combinedDocs.get(i);
            assertEquals("combined score of doc " + fieldDoc.doc, expectedCombinedScores[i], fieldDoc.score, DELTA_FOR_SCORE_ASSERTION);
            assertEquals(2, fieldDoc.fields.length);
            assertEquals("_score sort value of doc " + fieldDoc.doc, fieldDoc.score, ((Number) fieldDoc.fields[0]).floatValue(), 0.0f);
            assertEquals("tiebreaker of doc " + fieldDoc.doc, expectedPrices[i], ((Number) fieldDoc.fields[1]).intValue());
        }
    }

    public void testCombination_whenScorePrimaryWithSearchAfter_thenOnlyDocsAfterCursorKept() {
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Sort sort = new Sort(sortFields);

        // cursor = doc3's own sort values [0.5, 200]: strictly after it are doc1 (tie on 0.5, price 300 > 200) and doc4 (0.2)
        List<CompoundTopDocs> queryTopDocs = List.of(scorePrimaryShard(sortFields));
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(false)
                .searchAfterPerShard(List.of(new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 0.5f, 200 })))
                .build()
        );
        assertEquals(List.of(1, 4), queryTopDocs.getFirst().getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());

        // a cursor between two tied scores: [0.5, 250] keeps doc1 (price 300) and drops doc3 (price 200)
        queryTopDocs = List.of(scorePrimaryShard(sortFields));
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(false)
                .searchAfterPerShard(List.of(new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 0.5f, 250 })))
                .build()
        );
        assertEquals(List.of(1, 4), queryTopDocs.getFirst().getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());

        // the cursor of the last document leaves nothing: search_after is exclusive
        queryTopDocs = List.of(scorePrimaryShard(sortFields));
        scoreCombiner.combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(sort)
                .isSingleShard(false)
                .searchAfterPerShard(List.of(new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 0.2f, 50 })))
                .build()
        );
        assertTrue(queryTopDocs.getFirst().getScoreDocs().isEmpty());
    }

    public void testCombination_whenScorePrimaryAndBestCombinedScoreHeadsNoSubQueryList_thenOrderedByCombinedScore() {
        // [_score desc, price asc]: each sub-query list is in the order of that sub-query's own score, and the document with the
        // best combined score is second in both, so the lists are not sorted runs of the combined score.
        // doc1: [1.0, 0 ] price 100 -> mean 0.5
        // doc3: [0.8, 0.6] price 300 -> mean 0.7
        // doc4: [0 , 1.0] price 400 -> mean 0.5
        // combined order: doc3 (0.7), then doc1 and doc4 tie at 0.5 -> price asc. A merge of the lists as sorted runs would
        // put doc1 first.
        assertEquals(List.of(3, 1, 4), combineBestSecondInEveryList(null));
    }

    public void testCombination_whenScorePrimaryWithSearchAfterInsideTheOrder_thenWalkHasNoGapAndNoDuplicate() {
        // the cursors of the documents of the order above, one after the other: every page keeps exactly the documents after it
        assertEquals(List.of(1, 4), combineBestSecondInEveryList(new Object[] { 0.7f, 300 }));
        assertEquals(List.of(4), combineBestSecondInEveryList(new Object[] { 0.5f, 100 }));
        assertEquals(List.of(), combineBestSecondInEveryList(new Object[] { 0.5f, 400 }));
    }

    public void testCombination_whenScorePrimaryAndShardsMapTieBreakerWithDifferentTypes_thenEachShardComparesItsOwnType() {
        // two indices behind one search map the tie-breaker as integer and long: each shard's values and cursor have that
        // shard's type, and each shard is compared with its own sort fields
        SortField[] intSortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        SortField[] longSortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.LONG) };
        List<CompoundTopDocs> queryTopDocs = List.of(
            singleListShard(
                intSortFields,
                new FieldDoc(0, 1.0f, new Object[] { 1.0f, 100 }),
                new FieldDoc(1, 0.5f, new Object[] { 0.5f, 300 })
            ),
            singleListShard(
                longSortFields,
                new FieldDoc(0, 1.0f, new Object[] { 1.0f, 200L }),
                new FieldDoc(1, 0.5f, new Object[] { 0.5f, 50L })
            )
        );

        new ScoreCombiner().combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(new Sort(intSortFields))
                .isSingleShard(false)
                .searchAfterPerShard(
                    List.of(
                        new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 1.0f, 150 }),
                        new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 1.0f, 150L })
                    )
                )
                .build()
        );

        // [1.0, 150] is after doc0 of the integer shard (price 100) and before doc0 of the long shard (price 200)
        assertEquals(List.of(1), queryTopDocs.get(0).getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());
        assertEquals(List.of(0, 1), queryTopDocs.get(1).getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());
        assertEquals(200L, ((FieldDoc) queryTopDocs.get(1).getScoreDocs().getFirst()).fields[1]);
    }

    public void testCombination_whenScorePrimaryWithSearchAfterAndShardHasNoDocuments_thenNothingIsKept() {
        // a shard can count matches and still have no document in any sub-query's window, or no sub-query top docs at all
        // (issue 1934): there is nothing to order or to cut, and no sort fields to read
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new FieldDoc[0], sortFields),
                    new TopFieldDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new FieldDoc[0], sortFields)
                ),
                true,
                SEARCH_SHARD
            ),
            new CompoundTopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ArrayList<>(), true, SEARCH_SHARD),
            singleListShard(sortFields, new FieldDoc(0, 1.0f, new Object[] { 1.0f, 100 }), new FieldDoc(1, 0.5f, new Object[] { 0.5f, 50 }))
        );
        FieldDoc cursor = new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 1.0f, 100 });

        new ScoreCombiner().combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(new Sort(sortFields))
                .isSingleShard(false)
                .searchAfterPerShard(List.of(cursor, cursor, cursor))
                .build()
        );

        assertTrue(queryTopDocs.get(0).getScoreDocs().isEmpty());
        assertTrue(queryTopDocs.get(1).getScoreDocs().isEmpty());
        // the shard next to them is still cut: its first document is the cursor document itself
        assertEquals(List.of(1), queryTopDocs.get(2).getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());
    }

    public void testCombination_whenScorePrimaryWithCollapse_thenShardKeepsTheMergedOrderForTheCollapseExecutor() {
        // with collapse the collapse executor orders the groups by the combined score afterwards: the shard's documents stay in
        // the order of the merged sub-query lists, each with its combined score in the _score slot and its collapse value last
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new CollapseTopFieldDocs(
                        "category",
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(1, 1.0f, new Object[] { 1.0f, 100 }),
                            new FieldDoc(3, 0.8f, new Object[] { 0.8f, 300 }) },
                        sortFields,
                        new Object[] { new BytesRef("a"), new BytesRef("b") }
                    ),
                    new CollapseTopFieldDocs(
                        "category",
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(4, 1.0f, new Object[] { 1.0f, 400 }),
                            new FieldDoc(3, 0.6f, new Object[] { 0.6f, 300 }) },
                        sortFields,
                        new Object[] { new BytesRef("c"), new BytesRef("b") }
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );

        new ScoreCombiner().combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(new Sort(sortFields))
                .isSingleShard(false)
                .build()
        );

        List<ScoreDoc> combinedDocs = queryTopDocs.getFirst().getScoreDocs();
        // merged on the sub-query sort values: doc 1 (1.0, 100), doc 4 (1.0, 400), doc 3 (0.8, 300); combined, doc 3 is first
        assertEquals(List.of(1, 4, 3), combinedDocs.stream().map(scoreDoc -> scoreDoc.doc).toList());
        FieldDoc doc3 = (FieldDoc) combinedDocs.get(2);
        assertEquals(0.7f, doc3.score, DELTA_FOR_SCORE_ASSERTION);
        assertEquals(List.of(doc3.score, 300, new BytesRef("b")), List.of(doc3.fields));
    }

    public void testCombination_whenScorePrimaryAndDocumentsEqualOnEverySortValue_thenDocIdOrder() {
        // documents equal on the combined score and on the field keep doc id order, as in a Lucene sorted collector
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        List<CompoundTopDocs> queryTopDocs = List.of(
            singleListShard(
                sortFields,
                new FieldDoc(7, 1.0f, new Object[] { 1.0f, 100 }),
                new FieldDoc(2, 1.0f, new Object[] { 1.0f, 100 }),
                new FieldDoc(5, 0.5f, new Object[] { 0.5f, 100 })
            )
        );

        new ScoreCombiner().combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(new Sort(sortFields))
                .isSingleShard(false)
                .build()
        );

        assertEquals(List.of(2, 7, 5), queryTopDocs.getFirst().getScoreDocs().stream().map(scoreDoc -> scoreDoc.doc).toList());
    }

    private List<Integer> combineBestSecondInEveryList(final Object[] searchAfter) {
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        List<CompoundTopDocs> queryTopDocs = List.of(
            new CompoundTopDocs(
                new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(1, 1.0f, new Object[] { 1.0f, 100 }),
                            new FieldDoc(3, 0.8f, new Object[] { 0.8f, 300 }) },
                        sortFields
                    ),
                    new TopFieldDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new FieldDoc[] {
                            new FieldDoc(4, 1.0f, new Object[] { 1.0f, 400 }),
                            new FieldDoc(3, 0.6f, new Object[] { 0.6f, 300 }) },
                        sortFields
                    )
                ),
                true,
                SEARCH_SHARD
            )
        );
        new ScoreCombiner().combineScores(
            CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .querySearchResults(Collections.emptyList())
                .sort(new Sort(sortFields))
                .isSingleShard(false)
                .searchAfterPerShard(searchAfter == null ? null : List.of(new FieldDoc(Integer.MAX_VALUE, 0.0f, searchAfter)))
                .build()
        );
        List<ScoreDoc> combinedDocs = queryTopDocs.getFirst().getScoreDocs();
        for (ScoreDoc scoreDoc : combinedDocs) {
            FieldDoc fieldDoc = (FieldDoc) scoreDoc;
            assertEquals("_score sort value of doc " + fieldDoc.doc, fieldDoc.score, ((Number) fieldDoc.fields[0]).floatValue(), 0.0f);
        }
        return combinedDocs.stream().map(scoreDoc -> scoreDoc.doc).toList();
    }

    /** One shard, one sub-query, field docs holding its normalized scores. */
    private CompoundTopDocs singleListShard(final SortField[] sortFields, final FieldDoc... fieldDocs) {
        return new CompoundTopDocs(
            new TotalHits(fieldDocs.length, TotalHits.Relation.EQUAL_TO),
            List.of(new TopFieldDocs(new TotalHits(fieldDocs.length, TotalHits.Relation.EQUAL_TO), fieldDocs, sortFields)),
            true,
            SEARCH_SHARD
        );
    }

    /** One shard, two sub-queries, [_score, price] field docs holding the normalized per-sub-query scores; see the test above. */
    private CompoundTopDocs scorePrimaryShard(SortField[] sortFields) {
        return new CompoundTopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopFieldDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new FieldDoc[] {
                        new FieldDoc(1, 1.0f, new Object[] { 1.0f, 300 }),
                        new FieldDoc(3, 0.5f, new Object[] { 0.5f, 200 }),
                        new FieldDoc(2, 0.2f, new Object[] { 0.2f, 100 }) },
                    sortFields
                ),
                new TopFieldDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new FieldDoc[] {
                        new FieldDoc(2, 1.0f, new Object[] { 1.0f, 100 }),
                        new FieldDoc(3, 0.5f, new Object[] { 0.5f, 200 }),
                        new FieldDoc(4, 0.4f, new Object[] { 0.4f, 50 }) },
                    sortFields
                )
            ),
            true,
            SEARCH_SHARD
        );
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
