/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * A DTO to hold all the semantic parameters.
 */
@Getter
@Builder
public class SemanticParameters {
    private final String modelId;
    private final String searchModelId;
    private final String rawFieldType;
    private final String semanticInfoFieldName;
    private final ChunkingConfig chunkingConfig;
    private final String semanticFieldSearchAnalyzer;
    private final Map<String, Object> denseEmbeddingConfig;
    private final SparseEncodingConfig sparseEncodingConfig;

    public boolean isChunkingEnabled() {
        if (chunkingConfig == null) {
            return false;
        }
        return chunkingConfig.isEnabled();
    }
}
