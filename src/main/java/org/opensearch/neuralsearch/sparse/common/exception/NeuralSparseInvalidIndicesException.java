/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common.exception;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * This is a customized exception class for those invalid indices during warmup/clear cache
 */
public class NeuralSparseInvalidIndicesException extends RuntimeException {

    private final List<String> invalidIndices;

    public NeuralSparseInvalidIndicesException(List<String> invalidIndices, String message) {
        super(message);
        this.invalidIndices = Collections.unmodifiableList(invalidIndices);
    }

    /**
     * Returns the Invalid Index
     *
     * @return invalid index name
     */
    public List<String> getInvalidIndices() {
        return invalidIndices;
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s: %s", getClass().getName(), getMessage());
    }
}
