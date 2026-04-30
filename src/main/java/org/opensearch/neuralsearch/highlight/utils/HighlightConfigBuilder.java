/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

/**
 * Unified configuration builder for semantic highlighting.
 * Provides configuration building for both single and batch semantic highlighting modes.
 */
@Log4j2
public class HighlightConfigBuilder {

    /**
     * Build configuration from search request and response (for batch processing)
     * @param request the search request
     * @param response the search response
     * @return extracted and validated configuration
     */
    public static HighlightConfig buildFromSearchRequest(SearchRequest request, SearchResponse response) {
        try {
            if (request == null || request.source() == null) {
                return HighlightConfig.empty();
            }

            HighlightBuilder highlighter = request.source().highlighter();
            InnerHitsHighlightLocator.Location innerHitsLocation = null;

            // If no top-level semantic highlight, look inside nested.inner_hits for one.
            if (highlighter == null || HighlightExtractorUtils.extractSemanticField(highlighter) == null) {
                innerHitsLocation = InnerHitsHighlightLocator.find(request.source().query());
                if (innerHitsLocation != null) {
                    highlighter = innerHitsLocation.getHighlightBuilder();
                }
            }

            if (highlighter == null) {
                return HighlightConfig.empty();
            }

            String fieldName = HighlightExtractorUtils.extractSemanticField(highlighter);
            String modelId = HighlightExtractorUtils.extractModelId(highlighter);
            String queryText = HighlightExtractorUtils.extractQueryText(request);

            // Extract batch inference settings from options
            boolean batchInference = HighlightExtractorUtils.extractBatchInference(highlighter);
            int maxBatchSize = HighlightExtractorUtils.extractMaxBatchSize(highlighter);

            HighlightConfig config = HighlightConfig.builder()
                .fieldName(fieldName)
                .modelId(modelId)
                .queryText(queryText)
                .preTag(HighlightExtractorUtils.extractPreTag(highlighter))
                .postTag(HighlightExtractorUtils.extractPostTag(highlighter))
                .batchInference(batchInference)
                .maxBatchSize(maxBatchSize)
                .innerHitName(innerHitsLocation != null ? innerHitsLocation.getInnerHitName() : null)
                .nestedPath(innerHitsLocation != null ? innerHitsLocation.getNestedPath() : null)
                .build();

            // Validate the configuration
            return HighlightValidator.validate(config, response);

        } catch (Exception e) {
            log.error("Failed to extract highlight configuration", e);
            return HighlightConfig.invalid("Configuration extraction failed: " + e.getMessage());
        }
    }
}
