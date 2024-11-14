/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.pruning;

import org.apache.commons.lang.StringUtils;

/**
 * Enum representing different types of pruning methods for sparse vectors
 */
public enum PruningType {
    NONE("none"),
    TOP_K("top_k"),
    ALPHA_MASS("alpha_mass"),
    MAX_RATIO("max_ratio"),
    ABS_VALUE("abs_value");

    private final String value;

    PruningType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Get PruningType from string value
     *
     * @param value string representation of pruning type
     * @return corresponding PruningType enum
     * @throws IllegalArgumentException if value doesn't match any pruning type
     */
    public static PruningType fromString(String value) {
        if (StringUtils.isEmpty(value)) return NONE;
        for (PruningType type : PruningType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown pruning type: " + value);
    }
}
