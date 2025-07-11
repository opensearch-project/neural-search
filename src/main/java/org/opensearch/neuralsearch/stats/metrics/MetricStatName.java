/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.Version;
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
    MEMORY_SPARSE_TOTAL_USAGE("total_usage", "memory.sparse", MetricStatType.MEMORY, Version.V_3_2_0),
    MEMORY_SPARSE_FORWARD_INDEX_USAGE("forward_index_usage", "memory.sparse", MetricStatType.MEMORY, Version.V_3_2_0),
    MEMORY_SPARSE_CLUSTERED_POSTING_USAGE("clustered_posting_usage", "memory.sparse", MetricStatType.MEMORY, Version.V_3_2_0);

    private final String nameString;
    private final String path;
    private final MetricStatType statType;
    private MetricStat metricStat;
    private final Version version;

    /**
     * Enum lookup table by nameString
     */
    private static final Map<String, MetricStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.nameString, stat -> stat));

    /**
     * Determines whether a given string is a valid stat name
     * @param name
     * @return whether the name is valid
     */
    public static boolean isValidName(String name) {
        return BY_NAME.containsKey(name);
    }

    /**
     * Constructor
     * @param nameString the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     */
    MetricStatName(String nameString, String path, MetricStatType statType, Version version) {
        this.nameString = nameString;
        this.path = path;
        this.statType = statType;
        this.version = version;

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
    public Version version() {
        return Version.V_3_2_0;
    }

    @Override
    public String toString() {
        return getNameString();
    }
}
