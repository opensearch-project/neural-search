/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
            if (highlighter == null) {
                return HighlightConfig.empty();
            }

            List<String> semanticFields = extractAllSemanticFields(highlighter);
            if (semanticFields.isEmpty()) {
                return HighlightConfig.empty();
            }

            List<String> nestedPaths = HighlightExtractorUtils.extractNestedPaths(highlighter);
            List<String> innerHitsNames = HighlightExtractorUtils.extractInnerHitsNames(highlighter);

            String topLevelField = null;
            List<HighlightConfig.InnerHitsTarget> innerHitsTargets = new ArrayList<>();
            for (String field : semanticFields) {
                int pathIdx = HighlightExtractorUtils.matchNestedPathIndex(field, nestedPaths);
                if (pathIdx < 0) {
                    if (topLevelField == null) {
                        topLevelField = field;
                    } else {
                        log.warn("Multiple top-level semantic highlight fields declared; only [{}] will be used", topLevelField);
                    }
                } else {
                    String nestedPath = nestedPaths.get(pathIdx);
                    // inner_hits_names is optional; fall back to the nested path (OpenSearch's default bucket name)
                    String innerHitName = (pathIdx < innerHitsNames.size()) ? innerHitsNames.get(pathIdx) : nestedPath;
                    innerHitsTargets.add(new HighlightConfig.InnerHitsTarget(innerHitName, nestedPath, field));
                }
            }

            HighlightConfig config = HighlightConfig.builder()
                .fieldName(topLevelField)
                .modelId(HighlightExtractorUtils.extractModelId(highlighter))
                .queryText(HighlightExtractorUtils.extractQueryText(request))
                .preTag(HighlightExtractorUtils.extractPreTag(highlighter))
                .postTag(HighlightExtractorUtils.extractPostTag(highlighter))
                .batchInference(HighlightExtractorUtils.extractBatchInference(highlighter))
                .maxBatchSize(HighlightExtractorUtils.extractMaxBatchSize(highlighter))
                .innerHitsTargets(innerHitsTargets.isEmpty() ? null : innerHitsTargets)
                .build();

            return HighlightValidator.validate(config, response);

        } catch (Exception e) {
            log.error("Failed to extract highlight configuration", e);
            return HighlightConfig.invalid("Configuration extraction failed: " + e.getMessage());
        }
    }

    /**
     * Collect every semantic highlight field declared in the top-level highlighter.
     */
    private static List<String> extractAllSemanticFields(HighlightBuilder highlighter) {
        List<HighlightBuilder.Field> fields = Optional.ofNullable(highlighter.fields()).orElse(Collections.emptyList());
        List<String> semantic = new ArrayList<>();
        for (HighlightBuilder.Field f : fields) {
            if (SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(f.highlighterType())) {
                semantic.add(f.name());
            }
        }
        return semantic;
    }
}
