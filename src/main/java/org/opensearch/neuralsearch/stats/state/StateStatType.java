/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import org.opensearch.neuralsearch.stats.common.StatType;

import java.util.Locale;

/**
 * Enum for different kinds of state stat types to track
 */
public enum StateStatType implements StatType {
    COUNTABLE,
    SETTABLE;

    /**
     * Gets the name of the stat type, the enum name in lowercase
     * @return the name of the stat type
     */
    public String getName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
