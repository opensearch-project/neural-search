/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;

/**
 * DTO object to hold data required for Score Combination.
 */
@AllArgsConstructor
@Builder
@Getter
public class CombineScoresDto {
    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @NonNull
    private List<QuerySearchResult> querySearchResults;
    @Nullable
    private Sort sort;
    private int fromValueForSingleShard;
    private boolean isSingleShard;
}
