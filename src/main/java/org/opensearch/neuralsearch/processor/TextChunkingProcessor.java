/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.env.Environment;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.ProcessorDocumentUtils;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DEFAULT_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DISABLED_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseIntegerWithDefault;

/**
 * This processor is used for text chunking.
 * The text chunking results could be fed to downstream embedding processor.
 * The processor needs two fields: algorithm and field_map,
 * where algorithm defines chunking algorithm and parameters,
 * and field_map specifies which fields needs chunking and the corresponding keys for the chunking results.
 */
public final class TextChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_chunking";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String ALGORITHM_FIELD = "algorithm";
    private static final String DEFAULT_ALGORITHM = FixedTokenLengthChunker.ALGORITHM_NAME;
    public static final String IGNORE_MISSING = "ignore_missing";
    public static final boolean DEFAULT_IGNORE_MISSING = false;

    private int maxChunkLimit;
    private Chunker chunker;
    private final Map<String, Object> fieldMap;
    private final boolean ignoreMissing;
    private final ClusterService clusterService;
    private final AnalysisRegistry analysisRegistry;
    private final Environment environment;

    public TextChunkingProcessor(
        final String tag,
        final String description,
        final Map<String, Object> fieldMap,
        final Map<String, Object> algorithmMap,
        final boolean ignoreMissing,
        final Environment environment,
        final ClusterService clusterService,
        final AnalysisRegistry analysisRegistry
    ) {
        super(tag, description);
        this.fieldMap = fieldMap;
        this.ignoreMissing = ignoreMissing;
        this.environment = environment;
        this.clusterService = clusterService;
        this.analysisRegistry = analysisRegistry;
        parseAlgorithmMap(algorithmMap);
    }

    public String getType() {
        return TYPE;
    }

    // if ignore missing is true null fields return null. If ignore missing is false null fields return an empty list
    private boolean shouldProcessChunk(Object chunkObject) {
        return !ignoreMissing || Objects.nonNull(chunkObject);
    }

    @SuppressWarnings("unchecked")
    private void parseAlgorithmMap(final Map<String, Object> algorithmMap) {
        if (algorithmMap.size() > 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unable to create %s processor as [%s] contains multiple algorithms", TYPE, ALGORITHM_FIELD)
            );
        }

        String algorithmKey;
        Object algorithmValue;
        if (algorithmMap.isEmpty()) {
            algorithmKey = DEFAULT_ALGORITHM;
            algorithmValue = new HashMap<>();
        } else {
            Entry<String, Object> algorithmEntry = algorithmMap.entrySet().iterator().next();
            algorithmKey = algorithmEntry.getKey();
            algorithmValue = algorithmEntry.getValue();
            if (!(algorithmValue instanceof Map)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Unable to create %s processor as parameters for [%s] algorithm must be an object",
                        TYPE,
                        algorithmKey
                    )
                );
            }
        }

        if (!ChunkerFactory.CHUNKER_ALGORITHMS.contains(algorithmKey)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Chunking algorithm [%s] is not supported. Supported chunking algorithms are %s",
                    algorithmKey,
                    ChunkerFactory.CHUNKER_ALGORITHMS
                )
            );
        }
        Map<String, Object> chunkerParameters = (Map<String, Object>) algorithmValue;
        // parse processor level max chunk limit
        this.maxChunkLimit = parseIntegerWithDefault(chunkerParameters, MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
        if (maxChunkLimit <= 0 && maxChunkLimit != DISABLED_MAX_CHUNK_LIMIT) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Parameter [%s] must be positive or %s to disable this parameter",
                    MAX_CHUNK_LIMIT_FIELD,
                    DISABLED_MAX_CHUNK_LIMIT
                )
            );
        }
        // fixed token length algorithm needs analysis registry for tokenization
        chunkerParameters.put(FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        this.chunker = ChunkerFactory.create(algorithmKey, chunkerParameters);
    }

    @SuppressWarnings("unchecked")
    private boolean isListOfString(final Object value) {
        // an empty list is also List<String>
        if (!(value instanceof List)) {
            return false;
        }
        for (Object element : (List<Object>) value) {
            if (!(element instanceof String)) {
                return false;
            }
        }
        return true;
    }

    private int getMaxTokenCount(final Map<String, Object> sourceAndMetadataMap) {
        int defaultMaxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(environment.settings());
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (Objects.isNull(indexMetadata)) {
            return defaultMaxTokenCount;
        }
        // if the index is specified in the metadata, read maxTokenCount from the index setting
        return IndexSettings.MAX_TOKEN_COUNT_SETTING.get(indexMetadata.getSettings());
    }

    /**
     * This method will be invoked by PipelineService to perform chunking and then write back chunking results to the document.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     */
    @Override
    public IngestDocument execute(final IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        ProcessorDocumentUtils.validateMapTypeValue(
            FIELD_MAP_FIELD,
            sourceAndMetadataMap,
            fieldMap,
            indexName,
            clusterService,
            environment,
            true
        );
        // fixed token length algorithm needs runtime parameter max_token_count for tokenization
        Map<String, Object> runtimeParameters = new HashMap<>();
        int maxTokenCount = getMaxTokenCount(sourceAndMetadataMap);
        int chunkStringCount = getChunkStringCountFromMap(sourceAndMetadataMap, fieldMap);
        runtimeParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
        runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, chunkStringCount);
        chunkMapType(sourceAndMetadataMap, fieldMap, runtimeParameters);

        recordChunkingExecutionStats(chunker.getAlgorithmName());
        return ingestDocument;
    }

    @SuppressWarnings("unchecked")
    private int getChunkStringCountFromMap(Map<String, Object> sourceAndMetadataMap, final Map<String, Object> fieldMap) {
        int chunkStringCount = 0;
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                // call this method recursively when target key is a map
                Object sourceObject = sourceAndMetadataMap.get(originalKey);
                if (sourceObject instanceof List) {
                    List<Object> sourceObjectList = (List<Object>) sourceObject;
                    for (Object source : sourceObjectList) {
                        if (source instanceof Map) {
                            chunkStringCount += getChunkStringCountFromMap((Map<String, Object>) source, (Map<String, Object>) targetKey);
                        }
                    }
                } else if (sourceObject instanceof Map) {
                    chunkStringCount += getChunkStringCountFromMap((Map<String, Object>) sourceObject, (Map<String, Object>) targetKey);
                }
            } else {
                // chunk the object when target key is of leaf type (null, string and list of string)
                Object chunkObject = sourceAndMetadataMap.get(originalKey);
                chunkStringCount += getChunkStringCountFromLeafType(chunkObject);
            }
        }
        return chunkStringCount;
    }

    @SuppressWarnings("unchecked")
    private int getChunkStringCountFromLeafType(final Object value) {
        // leaf type means null, String or List<String>
        // the result should be an empty list when the input is null
        if (value instanceof String) {
            return StringUtils.isEmpty((String) value) ? 0 : 1;
        } else if (isListOfString(value)) {
            return (int) ((List<String>) value).stream().filter(s -> !StringUtils.isEmpty(s)).count();
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private void chunkMapType(
        Map<String, Object> sourceAndMetadataMap,
        final Map<String, Object> fieldMap,
        final Map<String, Object> runtimeParameters
    ) {
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                // call this method recursively when target key is a map
                Object sourceObject = sourceAndMetadataMap.get(originalKey);
                if (sourceObject instanceof List) {
                    List<Object> sourceObjectList = (List<Object>) sourceObject;
                    for (Object source : sourceObjectList) {
                        if (source instanceof Map) {
                            chunkMapType((Map<String, Object>) source, (Map<String, Object>) targetKey, runtimeParameters);
                        }
                    }
                } else if (sourceObject instanceof Map) {
                    chunkMapType((Map<String, Object>) sourceObject, (Map<String, Object>) targetKey, runtimeParameters);
                }
            } else {
                // chunk the object when target key is of leaf type (null, string and list of string)
                Object chunkObject = sourceAndMetadataMap.get(originalKey);

                if (shouldProcessChunk(chunkObject)) {
                    List<String> chunkedResult = chunkLeafType(chunkObject, runtimeParameters);
                    sourceAndMetadataMap.put(String.valueOf(targetKey), chunkedResult);
                }
            }
        }
    }

    /**
     * Chunk the content, update the runtime max_chunk_limit and return the result
     */
    private List<String> chunkString(final String content, final Map<String, Object> runTimeParameters) {
        // return an empty list for empty string
        if (StringUtils.isEmpty(content)) {
            return List.of();
        }
        List<String> contentResult = chunker.chunk(content, runTimeParameters);
        // update chunk_string_count for each string
        int chunkStringCount = parseInteger(runTimeParameters, CHUNK_STRING_COUNT_FIELD);
        runTimeParameters.put(CHUNK_STRING_COUNT_FIELD, chunkStringCount - 1);
        // update runtime max_chunk_limit if not disabled
        int runtimeMaxChunkLimit = parseInteger(runTimeParameters, MAX_CHUNK_LIMIT_FIELD);
        if (runtimeMaxChunkLimit != DISABLED_MAX_CHUNK_LIMIT) {
            runTimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit - contentResult.size());
        }
        return contentResult;
    }

    private List<String> chunkList(final List<String> contentList, final Map<String, Object> runTimeParameters) {
        // flatten original output format from List<List<String>> to List<String>
        List<String> result = new ArrayList<>();
        for (String content : contentList) {
            result.addAll(chunkString(content, runTimeParameters));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<String> chunkLeafType(final Object value, final Map<String, Object> runTimeParameters) {
        // leaf type means null, String or List<String>
        // the result should be an empty list when the input is null
        List<String> result = new ArrayList<>();
        if (value == null) {
            return result;
        }
        if (value instanceof String) {
            if (StringUtils.isBlank(String.valueOf(value))) {
                return result;
            }
            result = chunkString(value.toString(), runTimeParameters);
        } else if (isListOfString(value)) {
            result = chunkList((List<String>) value, runTimeParameters);
        }
        return result;
    }

    private void recordChunkingExecutionStats(String algorithmName) {
        EventStatsManager.increment(EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS);
        switch (algorithmName) {
            case DelimiterChunker.ALGORITHM_NAME -> EventStatsManager.increment(EventStatName.TEXT_CHUNKING_DELIMITER_EXECUTIONS);
            case FixedTokenLengthChunker.ALGORITHM_NAME -> EventStatsManager.increment(EventStatName.TEXT_CHUNKING_FIXED_LENGTH_EXECUTIONS);
        }
    }
}
