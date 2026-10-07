/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * How the native engine lays out the per-document forward vectors a seismic index scores
 * candidates against. Only the {@link SparseEngine#NATIVE} engine reads this.
 */
public enum SparseForwardIndex {
    /**
     * One forward index for the whole field, shared by every block (nsparse
     * {@code seismic_sq}).
     */
    SHARED("shared"),
    /**
     * Forward vectors stored inline with each block, read only for the blocks a query selects
     * (nsparse {@code disk_seismic_sq}).
     */
    PER_BLOCK("per_block");

    private final String name;

    SparseForwardIndex(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static SparseForwardIndex fromName(String name) {
        return Arrays.stream(values()).filter(f -> f.name.equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    /** The accepted values, for a validation error message. */
    public static String validNames() {
        return Arrays.stream(values()).map(SparseForwardIndex::getName).collect(Collectors.joining(", "));
    }

    public static final SparseForwardIndex DEFAULT = SHARED;
}
