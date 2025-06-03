/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.Map;

@Getter
public class CollapseDTO {
    // Base collapse fields
    private final List<CompoundTopDocs> collapseQueryTopDocs;
    private final List<QuerySearchResult> collapseQuerySearchResults;
    private final Sort collapseSort;
    private final int indexOfFirstNonEmpty;
    private final boolean isCollapseFetchPhaseExecuted;
    private final CombineScoresDto collapseCombineScoresDTO;

    // Shard-specific fields
    private List<? extends Map.Entry<?, FieldDoc>> relevantCollapseEntries;
    private String collapseField;
    private CompoundTopDocs updatedCollapseTopDocs;
    private int collapseShardIndex;

    private CollapseDTO(
        List<CompoundTopDocs> collapseQueryTopDocs,
        List<QuerySearchResult> collapseQuerySearchResults,
        Sort collapseSort,
        int indexOfFirstNonEmpty,
        boolean isCollapseFetchPhaseExecuted,
        CombineScoresDto collapseCombineScoresDTO
    ) {
        this.collapseQueryTopDocs = collapseQueryTopDocs;
        this.collapseQuerySearchResults = collapseQuerySearchResults;
        this.collapseSort = collapseSort;
        this.indexOfFirstNonEmpty = indexOfFirstNonEmpty;
        this.isCollapseFetchPhaseExecuted = isCollapseFetchPhaseExecuted;
        this.collapseCombineScoresDTO = collapseCombineScoresDTO;
    }

    public static CollapseDTO createInitialCollapseDTO(
        List<CompoundTopDocs> collapseQueryTopDocs,
        List<QuerySearchResult> collapseQuerySearchResults,
        Sort collapseSort,
        int indexOfFirstNonEmpty,
        boolean isCollapseFetchPhaseExecuted,
        CombineScoresDto collapseCombineScoresDTO
    ) {
        return new CollapseDTO(
            collapseQueryTopDocs,
            collapseQuerySearchResults,
            collapseSort,
            indexOfFirstNonEmpty,
            isCollapseFetchPhaseExecuted,
            collapseCombineScoresDTO
        );
    }

    public <T> void updateForShard(
        List<Map.Entry<T, FieldDoc>> relevantCollapseEntries,
        String collapseField,
        CompoundTopDocs updatedCollapseTopDocs,
        int collapseShardIndex
    ) {
        this.relevantCollapseEntries = relevantCollapseEntries;
        this.collapseField = collapseField;
        this.updatedCollapseTopDocs = updatedCollapseTopDocs;
        this.collapseShardIndex = collapseShardIndex;
    }

}
