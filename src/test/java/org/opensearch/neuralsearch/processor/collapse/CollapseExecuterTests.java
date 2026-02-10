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

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link CollapseExecutor}
 */
public class CollapseExecuterTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("test_index", 0, "test_node");
    private CollapseExecutor collapseExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        collapseExecutor = new CollapseExecutor();
    }

    /**
     * Test executeCollapse with BytesRef field type (keyword field)
     * Should create and use KeywordCollapseStrategy
     */
    public void testExecuteCollapse_whenBytesRefFieldType_thenUsesKeywordStrategy() {
        // Create test data with BytesRef collapse values (keyword field)
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "category",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, new BytesRef("TV") }),
                new FieldDoc(2, 0.8f, new Object[] { 0.8f, new BytesRef("Refrigerator") }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("TV"), new BytesRef("Refrigerator") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,  // isSortEnabled = true for CollapseTopFieldDocs
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            BytesRef.class  // Keyword field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        // There are 2 collapsed groups: TV and Refrigerator
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse with Long field type (numeric field)
     * Should create and use NumericCollapseStrategy
     */
    public void testExecuteCollapse_whenLongFieldType_thenUsesNumericStrategy() {
        // Create test data with Long collapse values (numeric field)
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "price_range",
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, 100L }),
                new FieldDoc(2, 0.9f, new Object[] { 0.9f, 200L }),
                new FieldDoc(3, 0.8f, new Object[] { 0.8f, 100L }) // Duplicate value
            },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 100L, 200L, 100L }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,  // isSortEnabled = true for CollapseTopFieldDocs
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            Long.class  // Numeric field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        // There are 2 collapsed groups: 100L and 200L
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse with Integer field type (numeric field)
     * Should create and use NumericCollapseStrategy
     */
    public void testExecuteCollapse_whenIntegerFieldType_thenUsesNumericStrategy() {
        // Create test data with Integer collapse values (numeric field)
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "age_group",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new FieldDoc(1, 1.0f, new Object[] { 1.0f, 25 }), new FieldDoc(2, 0.8f, new Object[] { 0.8f, 30 }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 25, 30 }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            Integer.class  // Numeric field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse with null field type
     * According to the code comment, when fieldType is null (cannot be determined),
     * it should default to NumericCollapseStrategy
     */
    public void testExecuteCollapse_whenNullFieldType_thenUsesNumericStrategy() {
        // Create test data with null collapse field type
        // This simulates the case where documents don't have the collapse field
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "missing_field",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new FieldDoc(1, 1.0f, new Object[] { 1.0f, null }), new FieldDoc(2, 0.8f, new Object[] { 0.8f, null }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { null, null }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            null  // Null field type - should default to NumericStrategy
        );

        // Execute collapse - should use NumericStrategy by default
        int result = collapseExecutor.executeCollapse(collapseDTO);
        // Both documents have null values, so they should be grouped together
        assertEquals(1, result);
    }

    /**
     * Test executeCollapse with Double field type (numeric field)
     * Should create and use NumericCollapseStrategy
     */
    public void testExecuteCollapse_whenDoubleFieldType_thenUsesNumericStrategy() {
        // Create test data with Double collapse values (numeric field)
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "rating",
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, 4.5 }),
                new FieldDoc(2, 0.9f, new Object[] { 0.9f, 3.5 }),
                new FieldDoc(3, 0.8f, new Object[] { 0.8f, 4.5 }) // Duplicate rating
            },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 4.5, 3.5, 4.5 }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            Double.class  // Numeric field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        // There are 2 collapsed groups: 4.5 and 3.5
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse with Float field type (numeric field)
     * Should create and use NumericCollapseStrategy
     */
    public void testExecuteCollapse_whenFloatFieldType_thenUsesNumericStrategy() {
        // Create test data with Float collapse values (numeric field)
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "score",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new FieldDoc(1, 1.0f, new Object[] { 1.0f, 9.5f }), new FieldDoc(2, 0.8f, new Object[] { 0.8f, 8.5f }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 9.5f, 8.5f }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            Float.class  // Numeric field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse with mixed null and non-null values in numeric field
     * Should handle null values gracefully in NumericStrategy
     */
    public void testExecuteCollapse_whenNullFieldTypeWithMixedValues_thenHandlesGracefully() {
        // Create test data with mixed null and non-null values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "optional_field",
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, 100L }),
                new FieldDoc(2, 0.9f, new Object[] { 0.9f, null }),
                new FieldDoc(3, 0.8f, new Object[] { 0.8f, 100L }) // Duplicate value
            },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 100L, null, 100L }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            null  // Null field type
        );

        // Execute collapse
        int result = collapseExecutor.executeCollapse(collapseDTO);
        // Should have 2 groups: one for 100L and one for null
        assertEquals(2, result);
    }

    /**
     * Test executeCollapse verifies that BytesRef uses KeywordStrategy
     * and any other type (including null) uses NumericStrategy
     */
    public void testExecuteCollapse_whenStringFieldType_thenUsesNumericStrategy() {
        // String.class is not BytesRef.class, so it should use NumericStrategy
        // This test verifies the logic: only BytesRef.class uses KeywordStrategy
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            "text_field",
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new FieldDoc(1, 1.0f, new Object[] { 1.0f, "value1" }),
                new FieldDoc(2, 0.8f, new Object[] { 0.8f, "value2" }) },
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { "value1", "value2" }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            true,
            SEARCH_SHARD
        );

        List<CompoundTopDocs> collapseQueryTopDocs = List.of(compoundTopDocs);
        List<QuerySearchResult> collapseQuerySearchResults = List.of(mock(QuerySearchResult.class));
        Sort collapseSort = new Sort(SortField.FIELD_SCORE);
        CombineScoresDto combineScoresDto = mock(CombineScoresDto.class);

        CollapseDTO collapseDTO = new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            true,
            combineScoresDto,
            String.class  // String.class is not BytesRef.class, so uses NumericStrategy
        );

        // Execute collapse - should use NumericStrategy (not KeywordStrategy)
        int result = collapseExecutor.executeCollapse(collapseDTO);
        assertEquals(2, result);
    }
}
