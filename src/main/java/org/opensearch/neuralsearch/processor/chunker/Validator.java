/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;

/**
 * An interface to define the behavior of the chunker validator
 */
@FunctionalInterface
public interface Validator {
    /**
     * Validate the parameters for a chunker
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters parameters for a chunker
     */
    void validate(Map<String, Object> parameters) throws IllegalArgumentException;
}
