/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all info stat names, paths, and types
 */
@Getter
public enum InfoStatName implements StatName {
    // Cluster info
    CLUSTER_VERSION("cluster_version", "", InfoStatType.INFO_STRING),
    TEXT_EMBEDDING_PROCESSORS("text_embedding_processors_in_pipelines", "processors.ingest", InfoStatType.INFO_COUNTER),
    TEXT_CHUNKING_PROCESSORS("text_chunking_processors", "processors.ingest", InfoStatType.INFO_COUNTER),
    TEXT_CHUNKING_DELIMITER_PROCESSORS("text_chunking_delimiter_processors", "processors.ingest", InfoStatType.INFO_COUNTER),
    TEXT_CHUNKING_FIXED_LENGTH_PROCESSORS("text_chunking_fixed_length_processors", "processors.ingest", InfoStatType.INFO_COUNTER);

    private final String nameString;
    private final String path;
    private final InfoStatType statType;

    private static final Map<String, InfoStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.nameString, stat -> stat));

    /**
     * Constructor
     * @param nameString the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     */
    InfoStatName(String nameString, String path, InfoStatType statType) {
        this.nameString = nameString;
        this.path = path;
        this.statType = statType;
    }

    /**
     * Gets the StatName associated with a unique string name
     * @throws IllegalArgumentException if stat name does not exist
     * @param value the string name of the stat
     * @return the StatName enum associated with that String name
     */
    public static InfoStatName from(String value) {
        if (BY_NAME.containsKey(value) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Info stat not found: %s", value));
        }
        return BY_NAME.get(value);
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
