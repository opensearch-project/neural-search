/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import org.opensearch.neuralsearch.stats.common.StatType;

import java.util.Locale;

public enum StateStatType implements StatType {
    COUNTABLE,
    SETTABLE;

    public String getName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
