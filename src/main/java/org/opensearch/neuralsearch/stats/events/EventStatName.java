/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public enum EventStatName {
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

    EventStatName(String name, String path, EventStatType statType) {
        this.name = name;
        this.path = path;
        this.statType = statType;
    }

    public static EventStatName from(String value) {
        if (BY_NAME.containsKey(value) == false) {
            throw new IllegalArgumentException(String.format("Event stat not found: %s", value));
        }
        return BY_NAME.get(value);
    }

    @Override
    public String toString() {
        return getName();
    }

    public String getFullPath() {
        if (StringUtils.isBlank(path)) {
            return name;
        }
        return String.join(".", path, name);
    }

}
