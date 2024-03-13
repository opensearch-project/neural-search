/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.math.NumberUtils;

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
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

/**
 * This processor is used for chunking user input data and chunked data could be used for downstream embedding processor,
 * algorithm can be used to indicate chunking algorithm and parameters,
 * and field_map can be used to indicate which fields needs chunking and the corresponding keys for the chunking results.
 */
public final class TextChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_chunking";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String ALGORITHM_FIELD = "algorithm";

    @VisibleForTesting
    static final String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    private static final int DEFAULT_MAX_CHUNK_LIMIT = -1;

    private int maxChunkLimit;

    private Chunker chunker;
    private final Map<String, Object> fieldMap;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    private final Environment environment;

    public TextChunkingProcessor(
        String tag,
        String description,
        Map<String, Object> fieldMap,
        Map<String, Object> algorithmMap,
        Environment environment,
        ClusterService clusterService,
        IndicesService indicesService,
        AnalysisRegistry analysisRegistry
    ) {
        super(tag, description);
        this.fieldMap = fieldMap;
        this.environment = environment;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
        validateAndParseAlgorithmMap(algorithmMap);
    }

    public String getType() {
        return TYPE;
    }

    @SuppressWarnings("unchecked")
    private void validateAndParseAlgorithmMap(Map<String, Object> algorithmMap) {
        if (algorithmMap.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unable to create %s processor as [%s] does not contain any algorithm", TYPE, ALGORITHM_FIELD)
            );
        } else if (algorithmMap.size() > 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unable to create %s processor as [%s] contain multiple algorithms", TYPE, ALGORITHM_FIELD)
            );
        }
        Entry<String, Object> algorithmEntry = algorithmMap.entrySet().iterator().next();
        String algorithmKey = algorithmEntry.getKey();
        Object algorithmValue = algorithmEntry.getValue();
        Set<String> supportedChunkers = ChunkerFactory.getAllChunkers();
        if (!supportedChunkers.contains(algorithmKey)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unable to create %s processor as chunker algorithm [%s] is not supported. Supported chunkers types are %s",
                    TYPE,
                    algorithmKey,
                    supportedChunkers
                )
            );
        }
        if (!(algorithmValue instanceof Map)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unable to create %s processor as [%s] parameters cannot be cast to [%s]",
                    TYPE,
                    algorithmKey,
                    Map.class.getName()
                )
            );
        }
        Map<String, Object> chunkerParameters = (Map<String, Object>) algorithmValue;
        if (Objects.equals(algorithmKey, FixedTokenLengthChunker.ALGORITHM_NAME)) {
            chunkerParameters.put(FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        }
        this.chunker = ChunkerFactory.create(algorithmKey, chunkerParameters);
        if (chunkerParameters.containsKey(MAX_CHUNK_LIMIT_FIELD)) {
            String maxChunkLimitString = chunkerParameters.get(MAX_CHUNK_LIMIT_FIELD).toString();
            if (!(NumberUtils.isParsable(maxChunkLimitString))) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Parameter [%s] cannot be cast to [%s]", MAX_CHUNK_LIMIT_FIELD, Number.class.getName())
                );
            }
            int maxChunkLimit = NumberUtils.createInteger(maxChunkLimitString);
            if (maxChunkLimit <= 0 && maxChunkLimit != DEFAULT_MAX_CHUNK_LIMIT) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Parameter [%s] must be a positive integer", MAX_CHUNK_LIMIT_FIELD)
                );
            }
            this.maxChunkLimit = maxChunkLimit;
        } else {
            this.maxChunkLimit = DEFAULT_MAX_CHUNK_LIMIT;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isListOfString(Object value) {
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

    private int chunkString(String content, List<String> result, Map<String, Object> runTimeParameters, int chunkCount) {
        // chunk the content, return the updated chunkCount and add chunk passages to result
        List<String> contentResult = chunker.chunk(content, runTimeParameters);
        chunkCount += contentResult.size();
        if (maxChunkLimit != DEFAULT_MAX_CHUNK_LIMIT && chunkCount > maxChunkLimit) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unable to chunk the document as the number of chunks [%s] exceeds the maximum chunk limit [%s]",
                    chunkCount,
                    maxChunkLimit
                )
            );
        }
        result.addAll(contentResult);
        return chunkCount;
    }

    private int chunkList(List<String> contentList, List<String> result, Map<String, Object> runTimeParameters, int chunkCount) {
        // flatten the List<List<String>> output to List<String>
        for (String content : contentList) {
            chunkCount = chunkString(content, result, runTimeParameters, chunkCount);
        }
        return chunkCount;
    }

    @SuppressWarnings("unchecked")
    private int chunkLeafType(Object value, List<String> result, Map<String, Object> runTimeParameters, int chunkCount) {
        // leaf type is either String or List<String>
        // the result should be an empty string
        if (value instanceof String) {
            chunkCount = chunkString(value.toString(), result, runTimeParameters, chunkCount);
        } else if (isListOfString(value)) {
            chunkCount = chunkList((List<String>) value, result, runTimeParameters, chunkCount);
        }
        return chunkCount;
    }

    private int getMaxTokenCount(Map<String, Object> sourceAndMetadataMap) {
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        int maxTokenCount;
        if (indexMetadata != null) {
            // if the index exists, read maxTokenCount from the index setting
            IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
            maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
        } else {
            maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(environment.settings());
        }
        return maxTokenCount;
    }

    /**
     * This method will be invoked by PipelineService to perform chunking and then write back chunking results to the document.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     */
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        validateFieldsValue(ingestDocument);
        int chunkCount = 0;
        Map<String, Object> runtimeParameters = new HashMap<>();
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        if (chunker instanceof FixedTokenLengthChunker) {
            int maxTokenCount = getMaxTokenCount(sourceAndMetadataMap);
            runtimeParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        }
        chunkMapType(sourceAndMetadataMap, fieldMap, runtimeParameters, chunkCount);
        return ingestDocument;
    }

    private void validateFieldsValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
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
    private void validateNestedTypeValue(String sourceKey, Object sourceValue, int maxDepth) {
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
    private void validateListTypeValue(String sourceKey, Object sourceValue, int maxDepth) {
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
        Map<String, Object> fieldMap,
        Map<String, Object> runtimeParameters,
        int chunkCount
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
                            chunkCount = chunkMapType(
                                (Map<String, Object>) source,
                                (Map<String, Object>) targetKey,
                                runtimeParameters,
                                chunkCount
                            );
                        }
                    }
                } else if (sourceObject instanceof Map) {
                    chunkCount = chunkMapType(
                        (Map<String, Object>) sourceObject,
                        (Map<String, Object>) targetKey,
                        runtimeParameters,
                        chunkCount
                    );
                }
            } else {
                // chunk the object when target key is a string
                Object chunkObject = sourceAndMetadataMap.get(originalKey);
                List<String> chunkedResult = new ArrayList<>();
                chunkCount = chunkLeafType(chunkObject, chunkedResult, runtimeParameters, chunkCount);
                sourceAndMetadataMap.put(String.valueOf(targetKey), chunkedResult);
            }
        }
        return chunkCount;
    }
}
