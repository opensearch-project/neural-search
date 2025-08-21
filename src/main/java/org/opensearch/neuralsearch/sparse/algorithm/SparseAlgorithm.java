/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

/**
 * Interface for sparse vector algorithms used in neural search.
 */
public interface SparseAlgorithm {
    /**
     * Validates the sparse method configuration.
     *
     * @param sparseMethodContext the method context to validate
     * @return validation exception if invalid, null if valid
     */
    ValidationException validateMethod(SparseMethodContext sparseMethodContext);
}
