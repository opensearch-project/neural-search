/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all metric stat names, paths, and types
 */
@Getter
public enum MetricStatName implements StatName {
    MEMORY_SPARSE_TOTAL_USAGE("total_usage", "memory.sparse", MetricStatType.MEMORY),
    MEMORY_SPARSE_FORWARD_INDEX_USAGE("forward_index_usage", "memory.sparse", MetricStatType.MEMORY),
    MEMORY_SPARSE_CLUSTERED_POSTING_USAGE("clustered_posting_usage", "memory.sparse", MetricStatType.MEMORY);

    private final String nameString;
    private final String path;
    private final MetricStatType statType;
    private MetricStat metricStat;

    /**
     * Enum lookup table by nameString
     */
    private static final Map<String, MetricStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.nameString, stat -> stat));

    /**
     * Constructor
     * @param nameString the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     */
    MetricStatName(String nameString, String path, MetricStatType statType) {
        this.nameString = nameString;
        this.path = path;
        this.statType = statType;

        switch (statType) {
            case MetricStatType.MEMORY:
                metricStat = new MemoryStat(this);
                break;
        }

        // Validates all event stats are instantiated correctly. This is covered by unit tests as well.
        if (metricStat == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unable to initialize metric stat [%s]. Unrecognized metric stat type: [%s]",
                    nameString,
                    statType
                )
            );
        }
    }

    /**
     * Gets the StatName associated with a unique string name
     * @throws IllegalArgumentException if stat name does not exist
     * @param name the string name of the stat
     * @return the StatName enum associated with that String name
     */
    public static MetricStatName from(String name) {
        if (!BY_NAME.containsKey(name)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Metric stat not found: %s", name));
        }
        return BY_NAME.get(name);
    }

    /**
     * Gets the full dot notation path of the stat, defining its location in the response body
     * @return the destination dot notation path of the stat value
     */
    public String getFullPath() {
        if (StringUtils.isBlank(path)) {
            return nameString;
        }
        return String.join(".", path, nameString);
    }

    @Override
    public String toString() {
        return getNameString();
    }
}
