/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;

import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
@Setter
/**
 *  Base abstract class for inference requests.
 *  This class contains common fields and behaviors shared across different types of inference requests.
 */
public abstract class InferenceRequest {
    /**
     * Unique identifier for the model to be used for inference.
     * This field is required and cannot be null.
     */
    @NonNull
    private String modelId;
    /**
     * List of targetResponseFilters to be applied.
     * Defaults value if not specified.
     */
    @Builder.Default
    private List<String> targetResponseFilters = List.of("sentence_embedding");
    /**
     * ML algorithm parameters for models.
     * For asymmetric models, use AsymmetricTextEmbeddingParameters with embeddingContentType set.
     */
    private MLAlgoParams mlAlgoParams;
    /**
     * Content type for embedding (QUERY or PASSAGE).
     * Used as indicator for asymmetric models to determine which prefix to apply.
     */
    private EmbeddingContentType embeddingContentType;
}
