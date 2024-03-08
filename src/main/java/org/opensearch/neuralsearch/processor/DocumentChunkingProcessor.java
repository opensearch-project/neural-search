/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.extern.log4j.Log4j2;
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
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FieldChunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.DELIMITER_ALGORITHM;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.FIXED_LENGTH_ALGORITHM;

/**
 * This processor is used for chunking user input data text embedding processing, algorithm can be used to indicate chunking algorithm and parameters,
 * and field_map can be used to indicate which fields needs chunking and the corresponding keys for the chunking results.
 */
@Log4j2
public final class DocumentChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "chunking";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String ALGORITHM_FIELD = "algorithm";

    public static String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    private static final int DEFAULT_MAX_CHUNK_LIMIT = -1;

    private int current_chunk_count = 0;

    private int max_chunk_limit = DEFAULT_MAX_CHUNK_LIMIT;
    private final Set<String> supportedChunkers = ChunkerFactory.getAllChunkers();

    private String chunkerType;

    private Map<String, Object> chunkerParameters;

    private final Map<String, Object> fieldMap;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    private final Environment environment;

    public DocumentChunkingProcessor(
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
        validateAndParseAlgorithmMap(algorithmMap);
        this.fieldMap = fieldMap;
        this.environment = environment;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    public String getType() {
        return TYPE;
    }

    @SuppressWarnings("unchecked")
    private void validateAndParseAlgorithmMap(Map<String, Object> algorithmMap) {
        if (algorithmMap.size() != 1) {
            throw new IllegalArgumentException(
                "Unable to create the processor as [" + ALGORITHM_FIELD + "] must contain and only contain 1 algorithm"
            );
        }

        for (Map.Entry<String, Object> algorithmEntry : algorithmMap.entrySet()) {
            String algorithmKey = algorithmEntry.getKey();
            Object algorithmValue = algorithmEntry.getValue();
            if (!supportedChunkers.contains(algorithmKey)) {
                throw new IllegalArgumentException(
                    "Unable to create the processor as chunker algorithm ["
                        + algorithmKey
                        + "] is not supported. Supported chunkers types are ["
                        + FIXED_LENGTH_ALGORITHM
                        + ", "
                        + DELIMITER_ALGORITHM
                        + "]"
                );
            }
            if (!(algorithmValue instanceof Map)) {
                throw new IllegalArgumentException(
                    "Unable to create the processor as [" + algorithmKey + "] parameters cannot be cast to [" + Map.class.getName() + "]"
                );
            }
            FieldChunker chunker = ChunkerFactory.create(algorithmKey, analysisRegistry);
            chunker.validateParameters((Map<String, Object>) algorithmValue);
            this.chunkerType = algorithmKey;
            this.chunkerParameters = (Map<String, Object>) algorithmValue;
            chunker.validateParameters(chunkerParameters);
            if (((Map<String, Object>) algorithmValue).containsKey(MAX_CHUNK_LIMIT_FIELD)) {
                int max_chunk_limit = ((Number) ((Map<String, Object>) algorithmValue).get(MAX_CHUNK_LIMIT_FIELD)).intValue();
                if (max_chunk_limit <= 0) {
                    throw new IllegalArgumentException("Parameter [" + MAX_CHUNK_LIMIT_FIELD + "] must be a positive integer");
                }
                this.max_chunk_limit = max_chunk_limit;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private boolean isListString(Object value) {
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

    private List<String> chunkString(String content) {
        FieldChunker chunker = ChunkerFactory.create(chunkerType, analysisRegistry);
        List<String> result = chunker.chunk(content, chunkerParameters);
        current_chunk_count += result.size();
        if (max_chunk_limit != DEFAULT_MAX_CHUNK_LIMIT && current_chunk_count > max_chunk_limit) {
            throw new IllegalArgumentException(
                "Unable to create the processor as the number of chunks ["
                    + current_chunk_count
                    + "] exceeds the maximum chunk limit ["
                    + max_chunk_limit
                    + "]"
            );
        }
        return result;
    }

    private List<String> chunkList(List<String> contentList) {
        // flatten the List<List<String>> output to List<String>
        List<String> result = new ArrayList<>();
        for (String content : contentList) {
            result.addAll(chunkString(content));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<String> chunkLeafType(Object value) {
        // leaf type is either String or List<String>
        List<String> chunkedResult = null;
        if (value instanceof String) {
            chunkedResult = chunkString(String.valueOf(value));
        } else if (isListString(value)) {
            chunkedResult = chunkList((List<String>) value);
        }
        return chunkedResult;
    }

    /**
     * This method will be invoked by PipelineService to perform chunking and then write back chunking results to the document.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     */
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        validateFieldsValue(ingestDocument);
        current_chunk_count = 0;
        if (Objects.equals(chunkerType, FIXED_LENGTH_ALGORITHM)) {
            // add maxTokenCount setting from index metadata to chunker parameters
            Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
            String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
            int maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(environment.settings());
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata != null) {
                // if the index exists, read maxTokenCount from the index setting
                IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
                maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
            }
            chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        }

        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        chunkMapType(sourceAndMetadataMap, fieldMap);
        sourceAndMetadataMap.forEach(ingestDocument::setFieldValue);
        return ingestDocument;
    }

    private void validateFieldsValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
                String sourceKey = embeddingFieldsEntry.getKey();
                Class<?> sourceValueClass = sourceValue.getClass();
                if (List.class.isAssignableFrom(sourceValueClass) || Map.class.isAssignableFrom(sourceValueClass)) {
                    validateNestedTypeValue(sourceKey, sourceValue, 1);
                } else if (!String.class.isAssignableFrom(sourceValueClass)) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] is neither string nor nested type, cannot process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(String sourceKey, Object sourceValue, int maxDepth) {
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reached max depth limit, cannot process it");
        } else if ((List.class.isAssignableFrom(sourceValue.getClass()))) {
            validateListTypeValue(sourceKey, sourceValue, maxDepth);
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values()
                .stream()
                .filter(Objects::nonNull)
                .forEach(x -> validateNestedTypeValue(sourceKey, x, maxDepth + 1));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has non-string type, cannot process it");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void validateListTypeValue(String sourceKey, Object sourceValue, int maxDepth) {
        for (Object value : (List) sourceValue) {
            if (value instanceof Map) {
                validateNestedTypeValue(sourceKey, value, maxDepth + 1);
            } else if (value == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, cannot process it");
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, cannot process it");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void chunkMapType(Map<String, Object> sourceAndMetadataMap, Map<String, Object> fieldMap) {
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
                            chunkMapType((Map<String, Object>) source, (Map<String, Object>) targetKey);
                        }
                    }
                } else if (sourceObject instanceof Map) {
                    chunkMapType((Map<String, Object>) sourceObject, (Map<String, Object>) targetKey);
                }
            } else {
                // chunk the object when target key is a string
                Object chunkObject = sourceAndMetadataMap.get(originalKey);
                List<String> chunkedResult = chunkLeafType(chunkObject);
                if (chunkedResult != null) {
                    sourceAndMetadataMap.put(String.valueOf(targetKey), chunkedResult);
                }
            }
        }
    }

    /**
     * Factory for chunking  ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
     */
    public static class Factory implements Processor.Factory {

        private final Environment environment;

        private final ClusterService clusterService;

        private final IndicesService indicesService;

        private final AnalysisRegistry analysisRegistry;

        public Factory(
            Environment environment,
            ClusterService clusterService,
            IndicesService indicesService,
            AnalysisRegistry analysisRegistry
        ) {
            this.environment = environment;
            this.clusterService = clusterService;
            this.indicesService = indicesService;
            this.analysisRegistry = analysisRegistry;
        }

        @Override
        public DocumentChunkingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            Map<String, Object> fieldMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
            Map<String, Object> algorithmMap = readMap(TYPE, processorTag, config, ALGORITHM_FIELD);
            return new DocumentChunkingProcessor(
                processorTag,
                description,
                fieldMap,
                algorithmMap,
                environment,
                clusterService,
                indicesService,
                analysisRegistry
            );
        }
    }
}
