/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.Optional;

@Builder
@AllArgsConstructor
@Getter
/**
 * DTO class to hold request parameters for normalization and combination
 */
public class NormalizationProcessorWorkflowExecuteRequest {
    final List<QuerySearchResult> querySearchResults;
    final Optional<FetchSearchResult> fetchSearchResultOptional;
    final ScoreNormalizationTechnique normalizationTechnique;
    final ScoreCombinationTechnique combinationTechnique;
    boolean explain;
    final PipelineProcessingContext pipelineProcessingContext;
    final SearchPhaseContext searchPhaseContext;
    final SearchContext searchContext;
    final FetchContext fetchContext;
}
