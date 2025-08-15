/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.Getter;
import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration of available sparse algorithm types for neural search.
 *
 * <p>This enum provides a registry of all supported sparse algorithms
 * and acts as a factory for algorithm instances. Each enum value
 * represents a specific algorithm implementation with its associated
 * name and validation logic.
 *
 * <p>Currently supported algorithms:
 * <ul>
 *   <li>SEISMIC - Sparse Efficient Index Search with Machine Intelligence and Clustering</li>
 * </ul>
 *
 * @see SparseAlgorithm
 * @see Seismic
 */
@Getter
public enum SparseAlgoType implements SparseAlgorithm {
    SEISMIC(SparseConstants.SEISMIC, Seismic.INSTANCE);

    private String name;
    private SparseAlgorithm algorithm;

    /**
     * Constructs a SparseAlgoType with the specified name and algorithm.
     *
     * @param name the algorithm name identifier
     * @param algorithm the algorithm implementation
     */
    SparseAlgoType(String name, SparseAlgorithm algorithm) {
        this.name = name;
        this.algorithm = algorithm;
    }

    /**
     * Returns the names of all available sparse algorithms.
     *
     * @return a set containing all algorithm names
     */
    public Set<String> getAllAlgos() {
        Set<String> allAlgos = new HashSet<>();
        for (SparseAlgoType sparseAlgoType : SparseAlgoType.values()) {
            allAlgos.add(sparseAlgoType.name);
        }
        return allAlgos;
    }

    /**
     * Delegates method validation to the underlying algorithm implementation.
     *
     * @param sparseMethodContext the method context to validate
     * @return ValidationException with errors, or null if valid
     */
    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        return algorithm.validateMethod(sparseMethodContext);
    }
}
