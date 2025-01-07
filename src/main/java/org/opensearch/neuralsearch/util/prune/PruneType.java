/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.prune;

import org.apache.commons.lang.StringUtils;

import java.util.Locale;

/**
 * Enum representing different types of prune methods for sparse vectors
 */
public enum PruneType {
    NONE("none"),
    TOP_K("top_k"),
    ALPHA_MASS("alpha_mass"),
    MAX_RATIO("max_ratio"),
    ABS_VALUE("abs_value");

    private final String value;

    PruneType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Get PruneType from string value
     *
     * @param value string representation of prune type
     * @return corresponding PruneType enum
     * @throws IllegalArgumentException if value doesn't match any prune type
     */
    public static PruneType fromString(final String value) {
        if (StringUtils.isEmpty(value)) return NONE;
        for (PruneType type : PruneType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Unknown prune type: %s", value));
    }
}
