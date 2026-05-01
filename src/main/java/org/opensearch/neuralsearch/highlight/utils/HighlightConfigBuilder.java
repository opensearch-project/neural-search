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

import java.util.ArrayList;
import java.util.List;

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
            String topLevelField = (highlighter != null) ? HighlightExtractorUtils.extractSemanticField(highlighter) : null;

            // Collect every semantic inner_hits target declared on nested queries.
            // A request may carry both a top-level semantic field and inner_hits
            // targets; both contribute to the final configuration.
            List<HighlightConfig.InnerHitsTarget> innerHitsTargets = new ArrayList<>();
            List<InnerHitsHighlightLocator.Location> locations = InnerHitsHighlightLocator.findAll(request.source().query());
            for (InnerHitsHighlightLocator.Location loc : locations) {
                String field = HighlightExtractorUtils.extractSemanticField(loc.getHighlightBuilder());
                if (field != null) {
                    innerHitsTargets.add(new HighlightConfig.InnerHitsTarget(loc.getInnerHitName(), loc.getNestedPath(), field));
                }
            }

            if (topLevelField == null && innerHitsTargets.isEmpty()) {
                return HighlightConfig.empty();
            }

            // Source global settings (model_id, tags, batch_inference) from the top-level
            // highlighter when present, otherwise from the first inner_hits highlighter.
            HighlightBuilder settingsSource = (topLevelField != null) ? highlighter : locations.get(0).getHighlightBuilder();

            HighlightConfig config = HighlightConfig.builder()
                .fieldName(topLevelField)
                .modelId(HighlightExtractorUtils.extractModelId(settingsSource))
                .queryText(HighlightExtractorUtils.extractQueryText(request))
                .preTag(HighlightExtractorUtils.extractPreTag(settingsSource))
                .postTag(HighlightExtractorUtils.extractPostTag(settingsSource))
                .batchInference(HighlightExtractorUtils.extractBatchInference(settingsSource))
                .maxBatchSize(HighlightExtractorUtils.extractMaxBatchSize(settingsSource))
                .innerHitsTargets(innerHitsTargets.isEmpty() ? null : innerHitsTargets)
                .build();

            return HighlightValidator.validate(config, response);

        } catch (Exception e) {
            log.error("Failed to extract highlight configuration", e);
            return HighlightConfig.invalid("Configuration extraction failed: " + e.getMessage());
        }
    }
}
