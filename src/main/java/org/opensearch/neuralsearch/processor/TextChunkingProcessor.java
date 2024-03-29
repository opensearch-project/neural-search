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

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.index.IndexSettings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DEFAULT_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DISABLED_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseIntegerParameter;

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

    private int maxChunkLimit;
    private Chunker chunker;
    private final Map<String, Object> fieldMap;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AnalysisRegistry analysisRegistry;
    private final Environment environment;

    public TextChunkingProcessor(
        final String tag,
        final String description,
        final Map<String, Object> fieldMap,
        final Map<String, Object> algorithmMap,
        final Environment environment,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AnalysisRegistry analysisRegistry
    ) {
        super(tag, description);
        this.fieldMap = fieldMap;
        this.environment = environment;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
        parseAlgorithmMap(algorithmMap);
    }

    public String getType() {
        return TYPE;
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
        this.maxChunkLimit = parseIntegerParameter(chunkerParameters, MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
        if (maxChunkLimit < 0 && maxChunkLimit != DISABLED_MAX_CHUNK_LIMIT) {
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
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (Objects.isNull(indexMetadata)) {
            return IndexSettings.MAX_TOKEN_COUNT_SETTING.get(environment.settings());
        }
        // if the index is specified in the metadata, read maxTokenCount from the index setting
        IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
        return indexService.getIndexSettings().getMaxTokenCount();
    }

    /**
     * This method will be invoked by PipelineService to perform chunking and then write back chunking results to the document.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     */
    @Override
    public IngestDocument execute(final IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        validateFieldsValue(sourceAndMetadataMap);
        // fixed token length algorithm needs runtime parameter max_token_count for tokenization
        Map<String, Object> runtimeParameters = new HashMap<>();
        int maxTokenCount = getMaxTokenCount(sourceAndMetadataMap);
        runtimeParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
        chunkMapType(sourceAndMetadataMap, fieldMap, runtimeParameters, 0);
        return ingestDocument;
    }

    private void validateFieldsValue(final Map<String, Object> sourceAndMetadataMap) {
        for (Map.Entry<String, Object> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (Objects.nonNull(sourceValue)) {
                String sourceKey = embeddingFieldsEntry.getKey();
                if (sourceValue instanceof List || sourceValue instanceof Map) {
                    validateNestedTypeValue(sourceKey, sourceValue, 1);
                } else if (!(sourceValue instanceof String)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "field [%s] is neither string nor nested type, cannot process it", sourceKey)
                    );
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(final String sourceKey, final Object sourceValue, final int maxDepth) {
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "map type field [%s] reached max depth limit, cannot process it", sourceKey)
            );
        } else if (sourceValue instanceof List) {
            validateListTypeValue(sourceKey, sourceValue, maxDepth);
        } else if (sourceValue instanceof Map) {
            ((Map) sourceValue).values()
                .stream()
                .filter(Objects::nonNull)
                .forEach(x -> validateNestedTypeValue(sourceKey, x, maxDepth + 1));
        } else if (!(sourceValue instanceof String)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "map type field [%s] has non-string type, cannot process it", sourceKey)
            );
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void validateListTypeValue(final String sourceKey, final Object sourceValue, final int maxDepth) {
        for (Object value : (List) sourceValue) {
            if (value instanceof Map) {
                validateNestedTypeValue(sourceKey, value, maxDepth + 1);
            } else if (value == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] has null, cannot process it", sourceKey)
                );
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "list type field [%s] has non-string value, cannot process it", sourceKey)
                );
            }
        }
    }

    @SuppressWarnings("unchecked")
    private int chunkMapType(
        Map<String, Object> sourceAndMetadataMap,
        final Map<String, Object> fieldMap,
        final Map<String, Object> runtimeParameters,
        final int chunkCount
    ) {
        int updatedChunkCount = chunkCount;
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
                            updatedChunkCount = chunkMapType(
                                (Map<String, Object>) source,
                                (Map<String, Object>) targetKey,
                                runtimeParameters,
                                updatedChunkCount
                            );
                        }
                    }
                } else if (sourceObject instanceof Map) {
                    updatedChunkCount = chunkMapType(
                        (Map<String, Object>) sourceObject,
                        (Map<String, Object>) targetKey,
                        runtimeParameters,
                        updatedChunkCount
                    );
                }
            } else {
                // chunk the object when target key is of leaf type (null, string and list of string)
                Object chunkObject = sourceAndMetadataMap.get(originalKey);
                List<String> chunkedResult = chunkLeafType(chunkObject, runtimeParameters);
                sourceAndMetadataMap.put(String.valueOf(targetKey), chunkedResult);
            }
        }
        return updatedChunkCount;
    }

    /**
     * Chunk the content, update the runtime max_chunk_limit and return the result
     */
    private List<String> chunkString(final String content, final Map<String, Object> runTimeParameters) {
        // update runtime max_chunk_limit if not disabled
        List<String> contentResult = chunker.chunk(content, runTimeParameters);
        int runtimeMaxChunkLimit = parseIntegerParameter(runTimeParameters, MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
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
        if (value instanceof String) {
            result = chunkString(value.toString(), runTimeParameters);
        } else if (isListOfString(value)) {
            result = chunkList((List<String>) value, runTimeParameters);
        }
        return result;
    }
}
