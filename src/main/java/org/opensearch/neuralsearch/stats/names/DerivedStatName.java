/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.names;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@Getter
public enum DerivedStatName {
    // Cluster info
    CLUSTER_VERSION("cluster_version", StatType.DERIVED_INFO_COUNTER),
    TEXT_EMBEDDING_PROCESSORS("processors.text_embedding_processors_in_pipelines", StatType.DERIVED_INFO_COUNTER);

    private final String name;
    private final StatType statType;

    DerivedStatName(String name, StatType statType) {
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
        for (DerivedStatName eventStatNames : DerivedStatName.values()) {
            names.add(eventStatNames.getName());
        }
        return names;
    }

    @Override
    public String toString() {
        return getName();
    }
}
