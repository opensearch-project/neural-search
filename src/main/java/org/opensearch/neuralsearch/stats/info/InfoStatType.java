/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.opensearch.neuralsearch.stats.common.StatType;

import java.util.Locale;

/**
 * Enum for different kinds of info stat types to track
 */
public enum InfoStatType implements StatType {
    INFO_COUNTER,
    INFO_STRING,
    INFO_BOOLEAN;

    /**
     * Gets the name of the stat type, the enum name in lowercase
     * @return the name of the stat type
     */
    public String getTypeString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
