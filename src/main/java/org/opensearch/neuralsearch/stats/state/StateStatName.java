/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@Getter
public enum StateStatName {
    // Cluster info
    CLUSTER_VERSION("cluster_version", StateStatType.COUNTER),
    TEXT_EMBEDDING_PROCESSORS("processors.text_embedding_processors_in_pipelines", StateStatType.COUNTER);

    private final String name;
    private final StateStatType statType;

    StateStatName(String name, StateStatType statType) {
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
        for (StateStatName eventStatNames : StateStatName.values()) {
            names.add(eventStatNames.getName());
        }
        return names;
    }

    @Override
    public String toString() {
        return getName();
    }

}
