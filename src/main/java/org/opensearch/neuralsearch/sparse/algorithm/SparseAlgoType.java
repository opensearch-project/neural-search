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

@Getter
public enum SparseAlgoType implements SparseAlgorithm {
    SEISMIC(SparseConstants.SEISMIC, Seismic.INSTANCE);

    private String name;
    private SparseAlgorithm algorithm;

    SparseAlgoType(String name, SparseAlgorithm algorithm) {
        this.name = name;
        this.algorithm = algorithm;
    }

    public Set<String> getAllAlgos() {
        Set<String> allAlgos = new HashSet<>();
        for (SparseAlgoType sparseAlgoType : SparseAlgoType.values()) {
            allAlgos.add(sparseAlgoType.name);
        }
        return allAlgos;
    }

    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        return algorithm.validateMethod(sparseMethodContext);
    }
}
