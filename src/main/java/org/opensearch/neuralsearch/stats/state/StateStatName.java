/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

@Getter
public enum StateStatName {
    // Cluster info
    CLUSTER_VERSION("cluster_version", "", StateStatType.SETTABLE),
    TEXT_EMBEDDING_PROCESSORS("text_embedding_processors_in_pipelines", "processors", StateStatType.COUNTABLE);

    private final String name;
    private final String path;
    private final StateStatType statType;

    StateStatName(String name, String path, StateStatType statType) {
        this.name = name;
        this.path = path;
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

    public String getFullPath() {
        if (StringUtils.isBlank(path)) {
            return name;
        }
        return String.join(".", path, name);
    }
}
