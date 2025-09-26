/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;

/**
 * Pure validation logic for highlight configurations.
 * Single responsibility: validation only, no extraction.
 */
@Log4j2
public class HighlightValidator {

    /**
     * Validate the highlight configuration against the search response
     * @param config the configuration to validate
     * @param response the search response to validate against
     * @return validated configuration (may have validation error set)
     */
    public HighlightConfig validate(HighlightConfig config, SearchResponse response) {
        // Check if already marked invalid from extraction
        if (config.getValidationError() != null) {
            return config;
        }

        // Validate required fields
        if (config.getFieldName() == null || config.getFieldName().isEmpty()) {
            return config.toBuilder().validationError("No semantic highlight field specified").build();
        }

        if (config.getModelId() == null || config.getModelId().isEmpty()) {
            return config.toBuilder().validationError("Model ID is required for semantic highlighting").build();
        }

        if (config.getQueryText() == null || config.getQueryText().isEmpty()) {
            return config.toBuilder().validationError("Query text is required for semantic highlighting").build();
        }

        // Validate response has hits
        if (response == null || response.getHits() == null || response.getHits().getHits().length == 0) {
            return config.toBuilder().validationError("No search hits to highlight").build();
        }

        // Validate batch size if batch inference is enabled
        if (config.isBatchInference()) {
            if (config.getMaxBatchSize() <= 0) {
                return config.toBuilder().validationError("Invalid batch size: " + config.getMaxBatchSize()).build();
            }
        }

        log.debug("Validation successful for field: {}, modelId: {}", config.getFieldName(), config.getModelId());

        return config; // Valid as-is
    }
}
