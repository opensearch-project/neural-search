/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

/**
 * Interface for sparse neural search algorithms.
 *
 * <p>This interface defines the contract for sparse algorithm implementations
 * used in neural search. Implementations must provide parameter validation
 * to ensure algorithm configurations are valid and safe.
 *
 * <p>Sparse algorithms optimize neural search performance by applying
 * various techniques such as clustering, approximation, and pruning
 * to sparse vector representations.
 *
 * @see SparseMethodContext
 * @see ValidationException
 */
public interface SparseAlgorithm {
    /**
     * Validates the algorithm method configuration.
     *
     * <p>Implementations should validate all algorithm-specific parameters
     * and return a ValidationException containing error messages if any
     * parameters are invalid. Returns null if validation passes.
     *
     * @param sparseMethodContext the method context containing parameters to validate
     * @return ValidationException with error messages, or null if valid
     */
    ValidationException validateMethod(SparseMethodContext sparseMethodContext);
}
