/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.Builder;
import lombok.Getter;
import lombok.With;

/**
 * Immutable configuration for semantic highlighting.
 * Replaces ValidationResult with a cleaner builder-based approach.
 * Fields can be null when configuration is invalid or incomplete.
 */
@Getter
@Builder(toBuilder = true)
public class HighlightConfig {

    private final String fieldName;

    private final String modelId;

    private final String queryText;

    @Builder.Default
    private final String preTag = SemanticHighlightingConstants.DEFAULT_PRE_TAG;

    @Builder.Default
    private final String postTag = SemanticHighlightingConstants.DEFAULT_POST_TAG;

    @Builder.Default
    private final boolean batchInference = false;

    @Builder.Default
    private final int maxBatchSize = SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;

    @With
    private final String validationError;

    /**
     * Check if the configuration is valid
     * @return true if no validation error exists and all required fields are present
     */
    public boolean isValid() {
        return validationError == null && fieldName != null && modelId != null && queryText != null;
    }

    /**
     * Create an invalid configuration with an error message
     * @param errorMessage the validation error message
     * @return invalid configuration
     */
    public static HighlightConfig invalid(String errorMessage) {
        return HighlightConfig.builder().validationError(errorMessage).build();
    }

    /**
     * Create an empty configuration (when no semantic field found)
     * @return empty configuration
     */
    public static HighlightConfig empty() {
        return HighlightConfig.builder().validationError("No semantic highlight field found").build();
    }
}
