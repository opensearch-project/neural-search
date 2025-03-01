/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.common;

/**
 * Interface for enums that hold the name of the stat.
 * The stat name is used as the unique identifier for the stat and as a rest path parameter for user filtering.
 * Each stat name should be used to track information and generate a single stat snapshot at a time.
 */
public interface StatName {
    /**
     * Gets the name of the stat. These must be unique to support user request stat filtering.
     * @return the name of the stat
     */
    String getName();

    /**
     * Gets the path of the stat in dot notation.
     * The path must be unique and avoid collisions with other stat names.
     * @return the path of the stat
     */
    String getFullPath();

    /**
     * The type of the stat
     * @return the stat type
     */
    StatType getStatType();
}
