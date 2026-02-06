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
            new Object[] { new BytesRef("electronics") }
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
}
