/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.Locale;

import lombok.Builder;
import lombok.Getter;
import lombok.With;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;

/**
 * Immutable configuration for semantic highlighting extracted and validated from search request options.
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

    @With
    private final FunctionName modelType;

    /**
     * Check if the configuration is valid
     * @return true if no validation error exists and all required fields are present
     */
    public boolean isValid() {
        return validationError == null && fieldName != null && modelId != null && queryText != null && validateBatchInference() == null;
    }

    /**
     * Check if configuration has required fields (before model type enrichment)
     * @return true if required fields are present
     */
    public boolean hasRequiredFields() {
        return fieldName != null && modelId != null && queryText != null;
    }

    /**
     * Check if the model supports batch inference
     * @return true if the model type is REMOTE, false otherwise
     */
    public boolean modelSupportsBatchInference() {
        return modelType == FunctionName.REMOTE;
    }

    /**
     * Validate batch inference configuration against model capabilities
     * @return validation error message if invalid, null if valid
     */
    public String validateBatchInference() {
        if (batchInference && modelType != null && !modelSupportsBatchInference()) {
            return String.format(
                Locale.ROOT,
                "Model [%s] with type [%s] does not support batch inference. "
                    + "Batch inference is only supported for REMOTE models. "
                    + "Please set 'batch_inference' to false or use a remote model.",
                modelId,
                modelType
            );
        }
        return null;
    }

    /**
     * Check if the configuration is invalid
     * @return true if validation error exists
     */
    public boolean isInvalid() {
        return validationError != null;
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
