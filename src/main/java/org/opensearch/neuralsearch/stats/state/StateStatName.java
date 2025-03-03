/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all state stat names, paths, and types
 */
@Getter
public enum StateStatName implements StatName {
    // Cluster info
    CLUSTER_VERSION("cluster_version", "", StateStatType.SETTABLE),
    TEXT_EMBEDDING_PROCESSORS("text_embedding_processors_in_pipelines", "processors.ingest", StateStatType.COUNTABLE);

    private final String name;
    private final String path;
    private final StateStatType statType;

    private static final Map<String, StateStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.name, stat -> stat));

    /**
     * Constructor
     * @param name the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     */
    StateStatName(String name, String path, StateStatType statType) {
        this.name = name;
        this.path = path;
        this.statType = statType;
    }

    /**
     * Gets the StatName associated with a unique string name
     * @throws IllegalArgumentException if stat name does not exist
     * @param name the string name of the stat
     * @return the StatName enum associated with that String name
     */
    public static StateStatName from(String value) {
        if (BY_NAME.containsKey(value) == false) {
            throw new IllegalArgumentException(String.format("State stat not found: %s", value));
        }
        return BY_NAME.get(value);
    }

    /**
     * Gets the full dot notation path of the stat, defining its location in the response body
     * @return the destination dot notation path of the stat value
     */
    public String getFullPath() {
        if (StringUtils.isBlank(path)) {
            return name;
        }
        return String.join(".", path, name);
    }

    @Override
    public String toString() {
        return getName();
    }
}
