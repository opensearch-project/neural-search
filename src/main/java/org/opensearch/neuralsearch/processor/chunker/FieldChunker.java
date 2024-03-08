/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;

/**
 * The interface for all chunking algorithms.
 * All algorithms need to validate parameters and chunk the content,
 */
public interface FieldChunker {

    /**
     * Chunk the incoming string and return chunked passages
     *
     * @param parameters a map containing parameters for chunking algorithms
     * @throws IllegalArgumentException parameters are invalid
     */
    void validateParameters(Map<String, Object> parameters);

    /**
     * Chunk the incoming string and return chunked passages
     *
     * @param content input string
     * @param parameters a map containing parameters for chunking algorithms
     * @return Chunked passages
     */
    List<String> chunk(String content, Map<String, Object> parameters);
}
