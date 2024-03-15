/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;

/**
 * The interface for all chunking algorithms.
 * All algorithms need to validate parameters and chunk the content.
 */
public interface Chunker {

    /**
     * Parse the parameters for chunking algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     * The parameters must be validated before parsing.
     *
     * @param parameters a map containing non-runtime parameters for chunking algorithms
     */
    void parseParameters(Map<String, Object> parameters);

    /**
     * Chunk the incoming string according to parameters and return chunked passages
     *
     * @param content input string
     * @param runtimeParameters a map containing runtime parameters for chunking algorithms
     * @return Chunked passages
     */
    List<String> chunk(String content, Map<String, Object> runtimeParameters);
}
