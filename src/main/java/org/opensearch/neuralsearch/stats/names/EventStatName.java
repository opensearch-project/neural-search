/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.names;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@Getter
public enum EventStatName {
    TEXT_EMBEDDING_PROCESSOR_EXECUTIONS("ingest_processor.text_embedding.executions", StatType.EVENT_COUNTER);

    private final String name;
    private final StatType statType;

    EventStatName(String name, StatType statType) {
        this.name = name;
        this.statType = statType;
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
}
