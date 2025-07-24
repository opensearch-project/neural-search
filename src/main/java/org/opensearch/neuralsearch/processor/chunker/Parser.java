/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;

/**
 * An interface to define the behavior of the chunker parser
 */
@FunctionalInterface
public interface Parser {
    /**
     * Parse the parameters for the chunking algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map containing non-runtime parameters for chunking algorithms
     */
    void parse(Map<String, Object> parameters);
}
