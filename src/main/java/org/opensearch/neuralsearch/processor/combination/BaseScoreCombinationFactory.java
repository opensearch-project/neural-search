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
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;

/**
 * Base class for creating score combination DTO.
 */
@Log4j2
public abstract class BaseScoreCombinationFactory {

    /**
     * DTO object to hold data required for Score Combination.
     */
    @AllArgsConstructor
    @Builder
    @Getter
    public static class CombineScoresDTO {
        @NonNull
        private List<CompoundTopDocs> queryTopDocs;
        @NonNull
        private ScoreCombinationTechnique scoreCombinationTechnique;
        @NonNull
        private List<QuerySearchResult> querySearchResults;
        @Nullable
        private Sort sort;
    }

}
