/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all event stat names, paths, and types
 */
@Getter
public enum EventStatName implements StatName {
    TEXT_EMBEDDING_PROCESSOR_EXECUTIONS("text_embedding_executions", "processors.ingest", EventStatType.TIMESTAMPED_COUNTER),
    TEST_EVENT_1("test_pasta_1", "test.events.my_event_1", EventStatType.TIMESTAMPED_COUNTER),
    TEST_EVENT_2("test_sushi_2", "test.events.my_event_1", EventStatType.TIMESTAMPED_COUNTER),
    TEST_EVENT_3("test_bratwurst_3", "test.events.my_event_2", EventStatType.TIMESTAMPED_COUNTER),
    TEST_EVENT_4("test_samosa_4", "test.events", EventStatType.TIMESTAMPED_COUNTER);

    private final String name;
    private final String path;
    private final EventStatType statType;

    private static final Map<String, EventStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.name, stat -> stat));

    /**
     * Constructor
     * @param name the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     */
    EventStatName(String name, String path, EventStatType statType) {
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
    public static EventStatName from(String name) {
        if (BY_NAME.containsKey(name) == false) {
            throw new IllegalArgumentException(String.format("Event stat not found: %s", name));
        }
        return BY_NAME.get(name);
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
