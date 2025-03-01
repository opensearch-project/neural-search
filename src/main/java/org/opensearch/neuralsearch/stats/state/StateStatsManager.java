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

public class StateStatsManager {
    public static final String PROCESSORS_KEY = "processors";
    public static final String ALGORITHM_KEY = "algorithm";

    // Search Response
    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    public static final String PHASE_PROCESSORS_KEY = "phase_results_processors";
    public static final String COMBINATION_KEY = "combination";
    public static final String NORMALIZATION_KEY = "normalization";
    public static final String TECHNIQUE_KEY = "technique";

    private static StateStatsManager INSTANCE;

    private NeuralSearchSettingsAccessor settingsAccessor;

    public static StateStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new StateStatsManager(NeuralSearchSettingsAccessor.instance());
        }
        return INSTANCE;
    }

    public StateStatsManager(NeuralSearchSettingsAccessor settingsAccessor) {
        this.settingsAccessor = settingsAccessor;
    }

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

    private Map<StateStatName, CountableStateStatSnapshot> getCountableStats() {
        // Initialize empty map with keys so stat names are visible in JSON even if the value is not counted
        Map<StateStatName, CountableStateStatSnapshot> countableStateStats = new HashMap<>();
        for (StateStatName stat : EnumSet.allOf(StateStatName.class)) {
            if (stat.getStatType() == StateStatType.COUNTABLE) {
                countableStateStats.put(stat, new CountableStateStatSnapshot(stat));
            }
        }

        // Parses search pipeline processor configs for processor info
        addSearchProcessorStats(countableStateStats);

        // Parses ingest pipeline processor configs for processor info
        addIngestProcessorStats(countableStateStats);
        return countableStateStats;
    }

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

    private void addClusterVersionStat(Map<StateStatName, SettableStateStatSnapshot<?>> stats) {
        StateStatName stateStatName = StateStatName.CLUSTER_VERSION;
        String version = NeuralSearchClusterUtil.instance().getClusterMinVersion().toString();
        stats.put(stateStatName, new SettableStateStatSnapshot<>(stateStatName, version));
    }

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

    private void addSearchProcessorStats(Map<StateStatName, CountableStateStatSnapshot> stats) {
        List<Map<String, Object>> pipelineConfigs = PipelineInfoUtil.instance().getSearchPipelineConfigs();

        for (Map<String, Object> pipelineConfig : pipelineConfigs) {
            for (Map.Entry<String, Object> entry : pipelineConfig.entrySet()) {
                String searchProcessorType = entry.getKey();
                List<Map<String, Object>> processors = asListOfMaps(entry.getValue());

                switch (searchProcessorType) {
                    case REQUEST_PROCESSORS_KEY:
                        countSearchRequestProcessors(stats, processors);
                        break;
                    case RESPONSE_PROCESSORS_KEY:
                        countSearchResponseProcessors(stats, processors);
                        break;
                    case PHASE_PROCESSORS_KEY:
                        countSearchPhaseResultsProcessors(stats, processors);
                        break;
                }
            }
        }
    }

    private void countSearchRequestProcessors(
        Map<StateStatName, CountableStateStatSnapshot> stats,
        List<Map<String, Object>> pipelineConfig
    ) {
        /*
        countProcessors(
            stats,
            pipelineConfig,
            NeuralQueryEnricherProcessor.TYPE,
            DerivedStatName.SEARCH_NEURAL_QUERY_ENRICHER_PROCESSOR_COUNT
        );
        */
        // Add additional processor cases here

    }

    private void countSearchResponseProcessors(
        Map<StateStatName, CountableStateStatSnapshot> stats,
        List<Map<String, Object>> pipelineConfig
    ) {
        // countProcessors(stats, pipelineConfig, ExplanationResponseProcessor.TYPE, DerivedStatName.SEARCH_EXPLANATION_PROCESSOR_COUNT);
        // Add additional processor cases here
    }

    private void countSearchPhaseResultsProcessors(
        Map<StateStatName, CountableStateStatSnapshot> stats,
        List<Map<String, Object>> pipelineConfig
    ) {
        // countProcessors(stats, pipelineConfig, RRFProcessor.TYPE, DerivedStatName.SEARCH_RRF_PROCESSOR_COUNT);

        // Add additional processor cases here
    }

    private void countProcessors(
        Map<StateStatName, CountableStateStatSnapshot> stats,
        List<Map<String, Object>> processors,
        String processorType,
        StateStatName stateStatName
    ) {
        long count = processors.stream().filter(p -> p.containsKey(processorType)).count();
        incrementBy(stats, stateStatName, count);
        // Add additional processor cases here
    }

    private void countCombinationTechniques(
        Map<StateStatName, CountableStateStatSnapshot> stats,
        List<Map<String, Object>> processors,
        String combinationTechnique,
        StateStatName stateStatName
    ) {
        // Parses to access combination technique field
        for (Map<String, Object> processorObj : processors) {
            Map<String, Object> processor = asMap(processorObj);
            for (Object processorConfigObj : processor.values()) {
                Map<String, Object> config = asMap(processorConfigObj);
                Map<String, Object> combination = asMap(config.get(COMBINATION_KEY));
                String technique = getValue(combination, TECHNIQUE_KEY, String.class);
                if (technique != null && technique.equals(combinationTechnique)) {
                    increment(stats, stateStatName);
                }
            }
        }
    }

    private void increment(Map<StateStatName, CountableStateStatSnapshot> stats, StateStatName stateStatName) {
        incrementBy(stats, stateStatName, 1L);
    }

    private void incrementBy(Map<StateStatName, CountableStateStatSnapshot> stats, StateStatName statName, Long amount) {
        if (stats.containsKey(statName)) {
            stats.get(statName).incrementBy(amount);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getValue(Map<String, Object> map, String key, Class<T> clazz) {
        if (map == null || key == null) return null;
        Object value = map.get(key);
        return clazz.isInstance(value) ? clazz.cast(value) : null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : null;
    }

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
