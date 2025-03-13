/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.neuralsearch.stats.common.StatType;

import java.util.Locale;

/**
 * Enum for different kinds of event stat types to track
 */
public enum EventStatType implements StatType {
    TIMESTAMPED_EVENT_COUNTER;

    /**
     * Gets the name of the stat type, the enum name in lowercase
     * @return the name of the stat type
     */
    public String getTypeString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
