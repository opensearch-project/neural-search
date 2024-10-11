/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.Optional;

/**
 * DTO object to hold data required for score normalization passed to execute() function
 * in NormalizationProcessorWorkflow. Field rankConstant
 * used in RRF but will be null for other normalization techniques
 */
@AllArgsConstructor
@Builder
@Getter
public class NormalizationExecuteDTO {
    @NonNull
    private List<QuerySearchResult> querySearchResults;
    @NonNull
    private Optional<FetchSearchResult> fetchSearchResultOptional;
    @NonNull
    private ScoreNormalizationTechnique normalizationTechnique;
    @NonNull
    private ScoreCombinationTechnique combinationTechnique;
}
