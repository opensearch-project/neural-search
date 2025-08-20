/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.Getter;
import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.algorithm.seismic.Seismic;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

/**
 * Enum registry of sparse algorithm types for neural search.
 */
@Getter
public enum SparseAlgoType implements SparseAlgorithm {
    SEISMIC(SparseConstants.SEISMIC, Seismic.INSTANCE);

    private String name;
    private SparseAlgorithm algorithm;

    /**
     * Creates a sparse algorithm type.
     *
     * @param name algorithm name
     * @param algorithm algorithm implementation
     */
    SparseAlgoType(String name, SparseAlgorithm algorithm) {
        this.name = name;
        this.algorithm = algorithm;
    }

    /**
     * Validates method configuration.
     *
     * @param sparseMethodContext method context to validate
     * @return validation exception if invalid, null if valid
     */
    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        return algorithm.validateMethod(sparseMethodContext);
    }
}
