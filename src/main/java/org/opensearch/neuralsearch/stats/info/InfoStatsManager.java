/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manager to generate stat snapshots for cluster level info stats
 */
public class InfoStatsManager {
    public static final String PROCESSORS_KEY = "processors";

    private final NeuralSearchClusterUtil neuralSearchClusterUtil;
    private final NeuralSearchSettingsAccessor settingsAccessor;
    private final PipelineServiceUtil pipelineServiceUtil;

    /**
     * Constructor
     *
     * @param settingsAccessor settings accessor singleton instance
     */
    public InfoStatsManager(
        NeuralSearchClusterUtil neuralSearchClusterUtil,
        NeuralSearchSettingsAccessor settingsAccessor,
        PipelineServiceUtil pipelineServiceUtil
    ) {
        this.neuralSearchClusterUtil = neuralSearchClusterUtil;
        this.settingsAccessor = settingsAccessor;
        this.pipelineServiceUtil = pipelineServiceUtil;
    }

    /**
     * Calculates and gets info stats
     * @param statsToRetrieve a set of the enums to retrieve
     * @return map of stat name to stat snapshot
     */
    public Map<InfoStatName, StatSnapshot<?>> getStats(EnumSet<InfoStatName> statsToRetrieve) {
        // info stats are calculated all at once regardless of filters
        Map<InfoStatName, CountableInfoStatSnapshot> countableInfoStats = getCountableStats();
        Map<InfoStatName, SettableInfoStatSnapshot<?>> settableInfoStats = getSettableStats();

        Map<InfoStatName, StatSnapshot<?>> prefilteredStats = new HashMap<>();
        prefilteredStats.putAll(countableInfoStats);
        prefilteredStats.putAll(settableInfoStats);

        // Filter based on specified stats
        Map<InfoStatName, StatSnapshot<?>> filteredStats = prefilteredStats.entrySet()
            .stream()
            .filter(entry -> statsToRetrieve.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return filteredStats;
    }

    /**
     * Calculates and gets info stats
     * @return map of stat name to stat snapshot
     */
    private Map<InfoStatName, CountableInfoStatSnapshot> getCountableStats() {
        // Initialize empty map with keys so stat names are visible in JSON even if the value is not counted
        Map<InfoStatName, CountableInfoStatSnapshot> countableInfoStats = new HashMap<>();
        for (InfoStatName stat : EnumSet.allOf(InfoStatName.class)) {
            if (stat.getStatType() == InfoStatType.INFO_COUNTER) {
                countableInfoStats.put(stat, new CountableInfoStatSnapshot(stat));
            }
        }

        // Parses ingest pipeline processor configs for processor info
        addIngestProcessorStats(countableInfoStats);

        // Helpers to parse search pipeline processor configs for processor info would go here
        return countableInfoStats;
    }

    /**
     * Calculates and gets settable info stats
     * @return map of stat name to stat snapshot
     */
    private Map<InfoStatName, SettableInfoStatSnapshot<?>> getSettableStats() {
        Map<InfoStatName, SettableInfoStatSnapshot<?>> settableInfoStats = new HashMap<>();
        for (InfoStatName statName : EnumSet.allOf(InfoStatName.class)) {
            switch (statName.getStatType()) {
                case InfoStatType.INFO_BOOLEAN -> settableInfoStats.put(statName, new SettableInfoStatSnapshot<Boolean>(statName));
                case InfoStatType.INFO_STRING -> settableInfoStats.put(statName, new SettableInfoStatSnapshot<String>(statName));
            }
        }

        addClusterVersionStat(settableInfoStats);
        return settableInfoStats;
    }

    /**
     * Adds cluster version to settable stats, mutating the input
     * @param stats mutable map of info stats that the result will be added to
     */
    private void addClusterVersionStat(Map<InfoStatName, SettableInfoStatSnapshot<?>> stats) {
        InfoStatName infoStatName = InfoStatName.CLUSTER_VERSION;
        String version = neuralSearchClusterUtil.getClusterMinVersion().toString();
        stats.put(infoStatName, new SettableInfoStatSnapshot<>(infoStatName, version));
    }

    /**
     * Adds ingest processor info stats, mutating the input
     * @param stats mutable map of info stats that the result will be added to
     */
    private void addIngestProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats) {
        List<Map<String, Object>> pipelineConfigs = pipelineServiceUtil.getIngestPipelineConfigs();

        // Iterate through all ingest processors and count their stats individually by calling helpers
        for (Map<String, Object> pipelineConfig : pipelineConfigs) {
            List<Map<String, Object>> ingestProcessors = asListOfMaps(pipelineConfig.get(PROCESSORS_KEY));
            for (Map<String, Object> ingestProcessor : ingestProcessors) {
                for (Map.Entry<String, Object> entry : ingestProcessor.entrySet()) {
                    String processorType = entry.getKey();
                    Map<String, Object> processorConfig = asMap(entry.getValue());
                    switch (processorType) {
                        case TextEmbeddingProcessor.TYPE:
                            increment(stats, InfoStatName.TEXT_EMBEDDING_PROCESSORS);
                            break;
                        case TextChunkingProcessor.TYPE:
                            countTextChunkingProcessorStats(stats, processorConfig);
                            break;
                    }
                }
            }
        }
    }

    /**
     * Counts text chunking processor stats based on processor config
     * @param stats
     * @param processorConfig
     */
    private void countTextChunkingProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        increment(stats, InfoStatName.TEXT_CHUNKING_PROCESSORS);

        Map<String, Object> algorithmMap = asMap(processorConfig.get(TextChunkingProcessor.ALGORITHM_FIELD));

        Map.Entry<String, Object> algorithmEntry = algorithmMap.entrySet().iterator().next();
        String algorithmKey = algorithmEntry.getKey();

        switch (algorithmKey) {
            case DelimiterChunker.ALGORITHM_NAME -> increment(stats, InfoStatName.TEXT_CHUNKING_DELIMITER_PROCESSORS);
            case FixedTokenLengthChunker.ALGORITHM_NAME -> increment(stats, InfoStatName.TEXT_CHUNKING_FIXED_LENGTH_PROCESSORS);
        }
    }

    /**
     * Increments a countable info stat in the given stat name
     * @param stats map containing the stat to increment
     * @param infoStatName the identifier for the stat to increment
     */
    private void increment(Map<InfoStatName, CountableInfoStatSnapshot> stats, InfoStatName infoStatName) {
        incrementBy(stats, infoStatName, 1L);
    }

    private void incrementBy(Map<InfoStatName, CountableInfoStatSnapshot> stats, InfoStatName statName, Long amount) {
        if (stats.containsKey(statName)) {
            stats.get(statName).incrementBy(amount);
        }
    }

    /**
     * Helper to cast generic object into a specific type
     * Used to parse pipeline processor configs
     * @param map the map
     * @param key the key
     * @param clazz the class to cast to
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
