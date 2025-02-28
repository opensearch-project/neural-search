/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.neuralsearch.stats.common.StatType;

import java.util.Locale;

public enum EventStatType implements StatType {
    TIMESTAMPED_COUNTER;

    public String getName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
