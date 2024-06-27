/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.search.query.QuerySearchResult;

/**
 * CombineScoresDTO handles the request sent for score combination after normalization.
 */
@AllArgsConstructor
public class CombineScoresDTO {
    @Getter
    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @Getter
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @Getter
    @NonNull
    private List<QuerySearchResult> querySearchResults;
    @Getter
    @Nullable
    private Sort sort;
}
