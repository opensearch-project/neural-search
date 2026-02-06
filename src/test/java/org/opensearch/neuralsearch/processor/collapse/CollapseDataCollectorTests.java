/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollapseDataCollectorTests extends OpenSearchTestCase {

    // Happy path cases
    public void testConstructor_whenBytesRefType_thenSuccessful() {
        CollapseTopFieldDocs testCollapseTopFieldDocs = new CollapseTopFieldDocs(
            null,
            null,
            null,
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("test") }
        );
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(null, List.of(testCollapseTopFieldDocs), null, null);

        CollapseDTO mockCollapseDTO = mock(CollapseDTO.class);
        when(mockCollapseDTO.getCollapseQueryTopDocs()).thenReturn(List.of(compoundTopDocs));
        when(mockCollapseDTO.getCollapseSort()).thenReturn(new Sort());
        CollapseDataCollector<?> collector = new CollapseDataCollector<>(mockCollapseDTO);
        assertNotNull(collector);
    }

    public void testConstructor_whenLongType_thenSuccessful() {
        CollapseTopFieldDocs testCollapseTopFieldDocs = new CollapseTopFieldDocs(
            null,
            null,
            null,
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 1L }
        );
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(null, List.of(testCollapseTopFieldDocs), null, null);

        CollapseDTO mockCollapseDTO = mock(CollapseDTO.class);
        when(mockCollapseDTO.getCollapseQueryTopDocs()).thenReturn(List.of(compoundTopDocs));
        when(mockCollapseDTO.getCollapseSort()).thenReturn(new Sort());
        CollapseDataCollector<?> collector = new CollapseDataCollector<>(mockCollapseDTO);
        assertNotNull(collector);
    }

    public void testCollectCollapseData_whenBytesRefValues_thenCollectsCorrectly() {
        // Create test data with BytesRef collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "category",
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, new BytesRef("TV") }),
                new FieldDoc(2, 0.8f, new Object[] { 0.8f, new BytesRef("Refrigerator") }),
                new FieldDoc(3, 0.9f, new Object[] { 0.9f, new BytesRef("Iron machine") }) // Duplicate category
            },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("TV"), new BytesRef("Refrigerator"), new BytesRef("Iron machine") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        // Verify collapse field is set
        assertEquals("category", collector.getCollapseField());

        // Verify sorted entries - should keep the best scoring doc for each category
        List<Map.Entry<BytesRef, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertEquals(3, sortedEntries.size()); // Two unique categories

        BytesRef electronicsKey = new BytesRef("TV");
        FieldDoc electronicsDoc = null;
        for (Map.Entry<BytesRef, FieldDoc> entry : sortedEntries) {
            if (entry.getKey().equals(electronicsKey)) {
                electronicsDoc = entry.getValue();
                break;
            }
        }
        assertNotNull(electronicsDoc);
        assertEquals(1, electronicsDoc.doc);
        assertEquals(1.0f, electronicsDoc.score, 0.001f);
    }

    public void testCollectCollapseData_whenLongValues_thenCollectsCorrectly() {
        // Create test data with Long collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "price_range",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new FieldDoc(1, 0.8f, new Object[] { 0.8f, 100L }), new FieldDoc(2, 0.9f, new Object[] { 0.9f, 200L }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 100L, 200L }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            Long.class
        );

        CollapseDataCollector<Long> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        assertEquals("price_range", collector.getCollapseField());

        List<Map.Entry<Long, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertEquals(2, sortedEntries.size());
    }

    public void testCollectCollapseData_whenMultipleShards_thenCollectsFromAllShards() {
        // Create test data for multiple shards
        CollapseTopFieldDocs shard1CollapseTopFieldDocs = new CollapseTopFieldDocs(
            "category",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, new BytesRef("electronics") }),
                new FieldDoc(2, 0.8f, new Object[] { 0.8f, new BytesRef("books") }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("electronics"), new BytesRef("books") }
        );

        CollapseTopFieldDocs shard2CollapseTopFieldDocs = new CollapseTopFieldDocs(
            "category",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(3, 0.9f, new Object[] { 0.9f, new BytesRef("electronics") }), // Lower score than shard1
                new FieldDoc(4, 0.7f, new Object[] { 0.7f, new BytesRef("clothing") }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("electronics"), new BytesRef("clothing") }
        );

        CompoundTopDocs shard1CompoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(shard1CollapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CompoundTopDocs shard2CompoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(shard2CollapseTopFieldDocs),
            true,
            new SearchShard("test_index", 1, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(shard1CompoundTopDocs, shard2CompoundTopDocs),
            List.of(mock(QuerySearchResult.class), mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        List<Map.Entry<BytesRef, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertEquals(3, sortedEntries.size()); // electronics, books, clothing

        // Electronics should come from shard1 (higher score)
        BytesRef electronicsKey = new BytesRef("electronics");
        Integer shardIndex = collector.getCollapseShardIndex(electronicsKey);
        assertEquals(Integer.valueOf(0), shardIndex); // First shard
    }

    // Edge cases
    public void testCollectCollapseData_whenEmptyResults_thenHandlesGracefully() {
        // Create test data with empty results
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "empty_field",
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            new SortField[] { SortField.FIELD_SCORE },
            new Object[0]
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        // Should handle empty results gracefully
        List<Map.Entry<BytesRef, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertTrue(sortedEntries.isEmpty());
    }

    public void testCollectCollapseData_whenNullCollapseValues_thenHandlesGracefully() {
        // Create test data with null collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "nullable_field",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, null }),
                new FieldDoc(2, 0.8f, new Object[] { 0.8f, new BytesRef("value") }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { null, new BytesRef("value") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        List<Map.Entry<BytesRef, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertEquals(2, sortedEntries.size()); // null and "value"
    }

    public void testCollectCollapseData_whenFieldDocHasEmptyFields_thenException() {
        // Create FieldDoc with empty fields array
        FieldDoc emptyFieldDoc = new FieldDoc(1, 1.0f, new Object[0]);

        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "test_field",
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { emptyFieldDoc },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("test") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        collector.collectCollapseData(collapseDTO);

        // Should skip the doc with empty fields
        List<Map.Entry<BytesRef, FieldDoc>> sortedEntries = collector.getSortedCollapseEntries();
        assertTrue(sortedEntries.isEmpty());
    }

    public void testCollectCollapseData_whenFieldDocHasNullFields_thenException() {
        // Create FieldDoc with null fields array
        FieldDoc nullFieldDoc = new FieldDoc(1, 1.0f, null);

        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "test_field",
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { nullFieldDoc },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("test") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            new SearchShard("test_index", 0, "test_node")
        );

        CollapseDTO collapseDTO = new CollapseDTO(
            List.of(compoundTopDocs),
            List.of(mock(QuerySearchResult.class)),
            new Sort(SortField.FIELD_SCORE),
            true,
            mock(CombineScoresDto.class),
            BytesRef.class
        );

        CollapseDataCollector<BytesRef> collector = new CollapseDataCollector<>(collapseDTO);
        assertThrows(IllegalArgumentException.class, () -> collector.collectCollapseData(collapseDTO));
    }

}
