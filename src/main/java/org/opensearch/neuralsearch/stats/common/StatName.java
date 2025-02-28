/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.common;

public interface StatName {
    String getName();

    String getFullPath();

    StatType getStatType();
}
