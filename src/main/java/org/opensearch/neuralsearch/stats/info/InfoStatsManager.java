/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NeuralSparseTwoPhaseProcessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import com.google.common.annotations.VisibleForTesting;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.GeometricMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.HarmonicMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedCharLengthChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Manager to generate stat snapshots for cluster level info stats
 */
public class InfoStatsManager {
    public static final String PROCESSORS_KEY = "processors";
    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    public static final String PHASE_PROCESSORS_KEY = "phase_results_processors";

    private final NeuralSearchClusterUtil neuralSearchClusterUtil;
    private final NeuralSearchSettingsAccessor settingsAccessor;
    private final PipelineServiceUtil pipelineServiceUtil;

    private static final Map<String, Consumer<Map<InfoStatName, CountableInfoStatSnapshot>>> chunkingAlgorithmIncrementers = Map.of(
        DelimiterChunker.ALGORITHM_NAME,
        stats -> increment(stats, InfoStatName.TEXT_CHUNKING_DELIMITER_PROCESSORS),
        FixedTokenLengthChunker.ALGORITHM_NAME,
        stats -> increment(stats, InfoStatName.TEXT_CHUNKING_FIXED_TOKEN_LENGTH_PROCESSORS),
        FixedCharLengthChunker.ALGORITHM_NAME,
        stats -> increment(stats, InfoStatName.TEXT_CHUNKING_FIXED_CHAR_LENGTH_PROCESSORS)
    );

    private static final Map<String, Consumer<Map<InfoStatName, CountableInfoStatSnapshot>>> normTechniqueIncrementers = Map.of(
        L2ScoreNormalizationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS),
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.NORM_TECHNIQUE_MINMAX_PROCESSORS),
        ZScoreNormalizationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.NORM_TECHNIQUE_ZSCORE_PROCESSORS)
    );

    private static final Map<String, Consumer<Map<InfoStatName, CountableInfoStatSnapshot>>> combTechniqueIncrementers = Map.of(
        ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.COMB_TECHNIQUE_ARITHMETIC_PROCESSORS),
        HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.COMB_TECHNIQUE_HARMONIC_PROCESSORS),
        GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.COMB_TECHNIQUE_GEOMETRIC_PROCESSORS),
        RRFScoreCombinationTechnique.TECHNIQUE_NAME,
        stats -> increment(stats, InfoStatName.COMB_TECHNIQUE_RRF_PROCESSORS)
    );

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

        // Parses search pipeline processor configs for processor info
        addSearchProcessorStats(countableInfoStats);

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
     * Adds search processor info stats, mutating the input
     * @param stats mutable map of info stats that the result will be added to
     */
    private void addSearchProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats) {
        List<Map<String, Object>> pipelineConfigs = pipelineServiceUtil.getSearchPipelineConfigs();

        // Iterate through all search processors and count their stats individually by calling helpers
        for (Map<String, Object> pipelineConfig : pipelineConfigs) {
            // Search request processors
            List<Map<String, Object>> requestProcessors = asListOfMaps(pipelineConfig.get(REQUEST_PROCESSORS_KEY));
            if (requestProcessors != null) {
                for (Map<String, Object> requestProcessor : requestProcessors) {
                    for (Map.Entry<String, Object> entry : requestProcessor.entrySet()) {
                        String processorType = entry.getKey();
                        Map<String, Object> processorConfig = asMap(entry.getValue());
                        switch (processorType) {
                            case NeuralQueryEnricherProcessor.TYPE -> increment(stats, InfoStatName.NEURAL_QUERY_ENRICHER_PROCESSORS);
                            case NeuralSparseTwoPhaseProcessor.TYPE -> increment(stats, InfoStatName.NEURAL_SPARSE_TWO_PHASE_PROCESSORS);
                        }
                    }
                }
            }

            // Search phase results processors
            List<Map<String, Object>> phaseResultsProcessors = asListOfMaps(pipelineConfig.get(PHASE_PROCESSORS_KEY));
            if (phaseResultsProcessors != null) {
                for (Map<String, Object> phaseResultsProcessor : phaseResultsProcessors) {
                    for (Map.Entry<String, Object> entry : phaseResultsProcessor.entrySet()) {
                        String processorType = entry.getKey();
                        Map<String, Object> processorConfig = asMap(entry.getValue());
                        switch (processorType) {
                            case NormalizationProcessor.TYPE -> countNormalizationProcessorStats(stats, processorConfig);
                            case RRFProcessor.TYPE -> countRRFProcessorStats(stats, processorConfig);
                        }
                    }
                }
            }

            // Search response processors
            List<Map<String, Object>> responseProcessors = asListOfMaps(pipelineConfig.get(RESPONSE_PROCESSORS_KEY));
            if (responseProcessors != null) {
                for (Map<String, Object> responseProcessor : responseProcessors) {
                    for (Map.Entry<String, Object> entry : responseProcessor.entrySet()) {
                        String processorType = entry.getKey();
                        Map<String, Object> processorConfig = asMap(entry.getValue());
                        switch (processorType) {
                            case RerankProcessor.TYPE -> countRerankProcessorStats(stats, processorConfig);
                        }
                    }
                }
            }
        }
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
            if (ingestProcessors != null) {
                for (Map<String, Object> ingestProcessor : ingestProcessors) {
                    for (Map.Entry<String, Object> entry : ingestProcessor.entrySet()) {
                        String processorType = entry.getKey();
                        Map<String, Object> processorConfig = asMap(entry.getValue());
                        switch (processorType) {
                            case TextEmbeddingProcessor.TYPE -> countTextEmbeddingProcessorStats(stats, processorConfig);
                            case TextImageEmbeddingProcessor.TYPE -> countTextImageEmbeddingProcessorStats(stats, processorConfig);
                            case TextChunkingProcessor.TYPE -> countTextChunkingProcessorStats(stats, processorConfig);
                            case SparseEncodingProcessor.TYPE -> countSparseEncodingProcessorStats(stats, processorConfig);
                        }
                    }
                }
            }
        }
    }

    /**
     * Counts text embedding processor stats based on processor config
     * @param stats map containing the stat to increment
     * @param processorConfig map of the processor config, parsed to add stats
     */
    private void countTextEmbeddingProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        increment(stats, InfoStatName.TEXT_EMBEDDING_PROCESSORS);
        Object skipExisting = processorConfig.get(TextEmbeddingProcessor.SKIP_EXISTING);
        if (Objects.nonNull(skipExisting) && skipExisting.equals(Boolean.TRUE)) {
            increment(stats, InfoStatName.SKIP_EXISTING_PROCESSORS);
        }
    }

    /**
     * Counts text/image embedding processor stats based on processor config
     * @param stats map containing the stat to increment
     * @param processorConfig map of the processor config, parsed to add stats
     */
    private void countTextImageEmbeddingProcessorStats(
        Map<InfoStatName, CountableInfoStatSnapshot> stats,
        Map<String, Object> processorConfig
    ) {
        increment(stats, InfoStatName.TEXT_IMAGE_EMBEDDING_PROCESSORS);
        Object skipExisting = processorConfig.get(TextImageEmbeddingProcessor.SKIP_EXISTING);
        if (Objects.nonNull(skipExisting) && skipExisting.equals(Boolean.TRUE)) {
            increment(stats, InfoStatName.SKIP_EXISTING_PROCESSORS);
        }
    }

    /**
     * Counts sparse encoding processor stats based on processor config
     * @param stats map containing the stat to increment
     * @param processorConfig map of the processor config, parsed to add stats
     */
    private void countSparseEncodingProcessorStats(
        Map<InfoStatName, CountableInfoStatSnapshot> stats,
        Map<String, Object> processorConfig
    ) {
        increment(stats, InfoStatName.SPARSE_ENCODING_PROCESSORS);
        Object skipExisting = processorConfig.get(SparseEncodingProcessor.SKIP_EXISTING);
        if (Objects.nonNull(skipExisting) && skipExisting.equals(Boolean.TRUE)) {
            increment(stats, InfoStatName.SKIP_EXISTING_PROCESSORS);
        }
    }

    /**
     * Counts text chunking processor stats based on processor config
     * @param stats map containing the stat to increment
     * @param processorConfig map of the processor config, parsed to add stats
     */
    private void countTextChunkingProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        increment(stats, InfoStatName.TEXT_CHUNKING_PROCESSORS);

        Map<String, Object> algorithmMap = asMap(processorConfig.get(TextChunkingProcessor.ALGORITHM_FIELD));

        Map.Entry<String, Object> algorithmEntry = algorithmMap.entrySet().iterator().next();
        String algorithmKey = algorithmEntry.getKey();

        // If no algorithm is specified, default case is fixed length
        if (chunkingAlgorithmIncrementers.containsKey(algorithmKey) == false) {
            increment(stats, InfoStatName.TEXT_CHUNKING_FIXED_TOKEN_LENGTH_PROCESSORS);
        } else {
            // Map is guaranteed to contain key in this block, so we can do direct map get
            chunkingAlgorithmIncrementers.get(algorithmKey).accept(stats);
        }
    }

    private void countNormalizationProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        increment(stats, InfoStatName.NORMALIZATION_PROCESSORS);

        String normalizationTechnique = asString(
            asMap(processorConfig.get(NormalizationProcessorFactory.NORMALIZATION_CLAUSE)).get(NormalizationProcessorFactory.TECHNIQUE)
        );
        String combinationTechnique = asString(
            asMap(processorConfig.get(NormalizationProcessorFactory.COMBINATION_CLAUSE)).get(NormalizationProcessorFactory.TECHNIQUE)
        );

        callNullableIncrementer(stats, normTechniqueIncrementers.get(normalizationTechnique));
        callNullableIncrementer(stats, combTechniqueIncrementers.get(combinationTechnique));
    }

    private void countRRFProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        increment(stats, InfoStatName.RRF_PROCESSORS);

        // RRF only has combination technique
        String combinationTechnique = asString(
            asMap(processorConfig.get(NormalizationProcessorFactory.COMBINATION_CLAUSE)).get(NormalizationProcessorFactory.TECHNIQUE)
        );

        callNullableIncrementer(stats, combTechniqueIncrementers.get(combinationTechnique));
    }

    private void countRerankProcessorStats(Map<InfoStatName, CountableInfoStatSnapshot> stats, Map<String, Object> processorConfig) {
        if (processorConfig.containsKey(RerankType.ML_OPENSEARCH.getLabel())) {
            increment(stats, InfoStatName.RERANK_ML_PROCESSORS);
        } else if (processorConfig.containsKey(RerankType.BY_FIELD.getLabel())) {
            increment(stats, InfoStatName.RERANK_BY_FIELD_PROCESSORS);
        }
    }

    /**
     * Increments a countable info stat in the given stat name
     * @param stats map containing the stat to increment
     * @param infoStatName the identifier for the stat to increment
     */
    @VisibleForTesting
    protected static void increment(Map<InfoStatName, CountableInfoStatSnapshot> stats, InfoStatName infoStatName) {
        incrementBy(stats, infoStatName, 1L);
    }

    private static void incrementBy(Map<InfoStatName, CountableInfoStatSnapshot> stats, InfoStatName statName, Long amount) {
        if (stats.containsKey(statName)) {
            stats.get(statName).incrementBy(amount);
        }
    }

    /**
     *  Conditionally accepts a param into a consumer if the consumer is non-null
     * In this class, used after getting a nullable stat incrementing consumer and safely incrementing it with
     * the ongoing stats map.
     *
     * @param stats the ongoing stats map
     * @param incrementer the consumer to increment the stat in the stats map
     */
    @VisibleForTesting
    protected void callNullableIncrementer(
        Map<InfoStatName, CountableInfoStatSnapshot> stats,
        Consumer<Map<InfoStatName, CountableInfoStatSnapshot>> incrementer
    ) {
        Optional.ofNullable(incrementer).ifPresent(consumer -> consumer.accept(stats));
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
     * Helper to cast generic object into String or null
     * Used to parse pipeline processor configs
     * @param value the object
     * @return the string or null if not a string
     */
    @SuppressWarnings("unchecked")
    private String asString(Object value) {
        return value instanceof String ? (String) value : null;
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
