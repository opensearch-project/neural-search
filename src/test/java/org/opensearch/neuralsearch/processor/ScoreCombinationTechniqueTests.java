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
}
