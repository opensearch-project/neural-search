/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

@Getter
public enum EventStatName {
    TEXT_EMBEDDING_PROCESSOR_EXECUTIONS("text_embedding.executions", "ingest_processor", EventStatType.TIMESTAMPED_COUNTER);

    private final String name;
    private final String path;
    private final EventStatType eventStatType;

    EventStatName(String name, String path, EventStatType eventStatType) {
        this.name = name;
        this.path = path;
        this.eventStatType = eventStatType;
    }

    /**
     * Get all stat names
     *
     * @return set of all stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();
        for (EventStatName eventStatName : EventStatName.values()) {
            names.add(eventStatName.getName());
        }
        return names;
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
