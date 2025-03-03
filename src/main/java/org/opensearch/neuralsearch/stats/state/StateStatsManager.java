/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineInfoUtil;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manager to generate stat snapshots for cluster level state stats
 */
public class StateStatsManager {
    public static final String PROCESSORS_KEY = "processors";

    private static StateStatsManager INSTANCE;

    private NeuralSearchSettingsAccessor settingsAccessor;

    /**
     * Creates or gets the singleton instance
     * @return singleton instance
     */
    public static StateStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new StateStatsManager(NeuralSearchSettingsAccessor.instance());
        }
        return INSTANCE;
    }

    /**
     * Constructor
     *
     * @param settingsAccessor settings accessor singleton instance
     */
    public StateStatsManager(NeuralSearchSettingsAccessor settingsAccessor) {
        this.settingsAccessor = settingsAccessor;
    }

    /**
     * Calculates and gets state stats
     * @param statsToRetrieve a set of the enums to retrieve
     * @return map of stat name to stat snapshot
     */
    public Map<StateStatName, StatSnapshot<?>> getStats(EnumSet<StateStatName> statsToRetrieve) {
        // State stats are calculated all at once regardless of filters
        Map<StateStatName, CountableStateStatSnapshot> countableStateStats = getCountableStats();
        Map<StateStatName, SettableStateStatSnapshot<?>> settableStateStats = getSettableStats();

        Map<StateStatName, StatSnapshot<?>> prefilteredStats = new HashMap<>();
        prefilteredStats.putAll(countableStateStats);
        prefilteredStats.putAll(settableStateStats);

        // Filter based on specified stats
        Map<StateStatName, StatSnapshot<?>> filteredStats = prefilteredStats.entrySet()
            .stream()
            .filter(entry -> statsToRetrieve.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return filteredStats;
    }

    /**
     * Calculates and gets state stats
     * @return map of stat name to stat snapshot
     */
    private Map<StateStatName, CountableStateStatSnapshot> getCountableStats() {
        // Initialize empty map with keys so stat names are visible in JSON even if the value is not counted
        Map<StateStatName, CountableStateStatSnapshot> countableStateStats = new HashMap<>();
        for (StateStatName stat : EnumSet.allOf(StateStatName.class)) {
            if (stat.getStatType() == StateStatType.COUNTABLE) {
                countableStateStats.put(stat, new CountableStateStatSnapshot(stat));
            }
        }

        // Parses ingest pipeline processor configs for processor info
        addIngestProcessorStats(countableStateStats);

        // Helpers to parse search pipeline processor configs for processor info would go here
        return countableStateStats;
    }

    /**
     * Calculates and gets settable state stats
     * @return map of stat name to stat snapshot
     */
    private Map<StateStatName, SettableStateStatSnapshot<?>> getSettableStats() {
        Map<StateStatName, SettableStateStatSnapshot<?>> settableStateStats = new HashMap<>();
        for (StateStatName statName : EnumSet.allOf(StateStatName.class)) {
            if (statName.getStatType() == StateStatType.SETTABLE) {
                settableStateStats.put(statName, new SettableStateStatSnapshot<>(statName));
            }
        }

        addClusterVersionStat(settableStateStats);
        return settableStateStats;
    }

    /**
     * Adds cluster version to settable stats, mutating the input
     * @param stats mutable map of state stats that the result will be added to
     */
    private void addClusterVersionStat(Map<StateStatName, SettableStateStatSnapshot<?>> stats) {
        StateStatName stateStatName = StateStatName.CLUSTER_VERSION;
        String version = NeuralSearchClusterUtil.instance().getClusterMinVersion().toString();
        stats.put(stateStatName, new SettableStateStatSnapshot<>(stateStatName, version));
    }

    /**
     * Adds ingest processor state stats, mutating the input
     * @param stats mutable map of state stats that the result will be added to
     */
    private void addIngestProcessorStats(Map<StateStatName, CountableStateStatSnapshot> stats) {
        List<Map<String, Object>> pipelineConfigs = PipelineInfoUtil.instance().getIngestPipelineConfigs();

        for (Map<String, Object> pipelineConfig : pipelineConfigs) {
            List<Map<String, Object>> ingestProcessors = asListOfMaps(pipelineConfig.get(PROCESSORS_KEY));
            for (Map<String, Object> ingestProcessor : ingestProcessors) {
                for (Map.Entry<String, Object> entry : ingestProcessor.entrySet()) {
                    String processorType = entry.getKey();
                    Map<String, Object> processorConfig = asMap(entry.getValue());
                    switch (processorType) {
                        case TextEmbeddingProcessor.TYPE:
                            increment(stats, StateStatName.TEXT_EMBEDDING_PROCESSORS);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Increments a countable state stat in the given stat name
     * @param stats map containing the stat to increment
     * @param stateStatName the identifier for the stat to increment
     */
    private void increment(Map<StateStatName, CountableStateStatSnapshot> stats, StateStatName stateStatName) {
        incrementBy(stats, stateStatName, 1L);
    }

    private void incrementBy(Map<StateStatName, CountableStateStatSnapshot> stats, StateStatName statName, Long amount) {
        if (stats.containsKey(statName)) {
            stats.get(statName).incrementBy(amount);
        }
    }

    /**
     * Helper to cast generic object into a specific type
     * Used to parse pipeline processor configs
     * @param value the object
     * @return the map
     */
    @SuppressWarnings("unchecked")
    private <T> T getValue(Map<String, Object> map, String key, Class<T> clazz) {
        if (map == null || key == null) return null;
        Object value = map.get(key);
        return clazz.isInstance(value) ? clazz.cast(value) : null;
    }

    /**
     * Helper to cast generic object into Map<String, Object>
     * Used to parse pipeline processor configs
     * @param value the object
     * @return the map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

    /**
     * Helper to cast generic object into a list of Map<String, Object>
     * Used to parse pipeline processor configs
     * @param value the object
     * @return the list of maps
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> asListOfMaps(Object value) {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            for (Object item : list) {
                if (!(item instanceof Map)) return null;
            }
            return (List<Map<String, Object>>) value;
        }
        return null;
    }
}
