/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

/**
 * Enum to represent the content type for text embedding.
 * Used to distinguish between query and passage/document embeddings in asymmetric models.
 */
public enum EmbeddingContentType {
    QUERY,
    PASSAGE;
}
