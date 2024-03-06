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
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.index.IndexSettings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.IFieldChunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.DELIMITER_ALGORITHM;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.FIXED_LENGTH_ALGORITHM;

@Log4j2
public final class DocumentChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "chunking";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String ALGORITHM_FIELD = "algorithm";

    private final Set<String> supportedChunkers = ChunkerFactory.getAllChunkers();

    private final Settings settings;

    private String chunkerType;

    private Map<String, Object> chunkerParameters;

    private final Map<String, Object> fieldMap;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    public DocumentChunkingProcessor(
        String tag,
        String description,
        Map<String, Object> fieldMap,
        Map<String, Object> algorithmMap,
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        AnalysisRegistry analysisRegistry
    ) {
        super(tag, description);
        validateAndParseAlgorithmMap(algorithmMap);
        this.fieldMap = fieldMap;
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    public String getType() {
        return TYPE;
    }

    private List<String> chunk(String content) {
        // assume that content is either a map, list or string
        IFieldChunker chunker = ChunkerFactory.create(chunkerType, analysisRegistry);
        return chunker.chunk(content, chunkerParameters);
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
                    "Unable to create the processor as [" + ALGORITHM_FIELD + "] cannot be cast to [" + Map.class.getName() + "]"
                );
            }
            this.chunkerType = algorithmKey;
            this.chunkerParameters = (Map<String, Object>) algorithmValue;
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Map<String, Object> processMap = buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
        List<String> inferenceList = createInferenceList(processMap);
        if (inferenceList.isEmpty()) {
            return ingestDocument;
        } else {
            return doExecute(ingestDocument, processMap, inferenceList);
        }
    }

    public IngestDocument doExecute(IngestDocument ingestDocument, Map<String, Object> ProcessMap, List<String> inferenceList) {
        if (Objects.equals(chunkerType, FIXED_LENGTH_ALGORITHM)) {
            // add maxTokenCount setting from index metadata to chunker parameters
            Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
            String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
            int maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings);
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata != null) {
                // if the index exists, read maxTokenCount from the index setting
                IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
                maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
            }
            chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        }

        List<List<String>> chunkedResults = new ArrayList<>();
        for (String inferenceString : inferenceList) {
            chunkedResults.add(chunk(inferenceString));
        }
        setTargetFieldsToDocument(ingestDocument, ProcessMap, chunkedResults);
        return ingestDocument;
    }

    private List<?> buildResultForListType(List<Object> sourceValue, List<?> results, InferenceProcessor.IndexWrapper indexWrapper) {
        Object peek = sourceValue.get(0);
        if (peek instanceof String) {
            List<Object> keyToResult = new ArrayList<>();
            IntStream.range(0, sourceValue.size()).forEachOrdered(x -> keyToResult.add(results.get(indexWrapper.index++)));
            return keyToResult;
        } else {
            List<List<Object>> keyToResult = new ArrayList<>();
            for (Object nestedList : sourceValue) {
                List<Object> nestedResult = new ArrayList<>();
                IntStream.range(0, ((List) nestedList).size()).forEachOrdered(x -> nestedResult.add(results.get(indexWrapper.index++)));
                keyToResult.add(nestedResult);
            }
            return keyToResult;
        }
    }

    private Map<String, Object> buildMapWithProcessorKeyAndOriginalValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> mapWithProcessorKeys = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildMapWithProcessorKeyAndOriginalValueForMapType(originalKey, targetKey, sourceAndMetadataMap, treeRes);
                mapWithProcessorKeys.put(originalKey, treeRes.get(originalKey));
            } else {
                mapWithProcessorKeys.put(String.valueOf(targetKey), sourceAndMetadataMap.get(originalKey));
            }
        }
        return mapWithProcessorKeys;
    }

    private void buildMapWithProcessorKeyAndOriginalValueForMapType(
        String parentKey,
        Object processorKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> treeRes
    ) {
        if (processorKey == null || sourceAndMetadataMap == null) return;
        if (processorKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            if (sourceAndMetadataMap.get(parentKey) instanceof Map) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                        next
                    );
                }
            } else if (sourceAndMetadataMap.get(parentKey) instanceof List) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) sourceAndMetadataMap.get(parentKey);
                    List<Object> listOfStrings = list.stream().map(x -> x.get(nestedFieldMapEntry.getKey())).collect(Collectors.toList());
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put(nestedFieldMapEntry.getKey(), listOfStrings);
                    buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        map,
                        next
                    );
                }
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(processorKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> createInferenceList(Map<String, Object> knnKeyMap) {
        List<String> texts = new ArrayList<>();
        knnKeyMap.entrySet().stream().filter(knnMapEntry -> knnMapEntry.getValue() != null).forEach(knnMapEntry -> {
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof List) {
                for (Object nestedValue : (List<Object>) sourceValue) {
                    if (nestedValue instanceof String) {
                        texts.add((String) nestedValue);
                    } else {
                        texts.addAll((List<String>) nestedValue);
                    }
                }
            } else if (sourceValue instanceof Map) {
                createInferenceListForMapTypeInput(sourceValue, texts);
            } else {
                texts.add(sourceValue.toString());
            }
        });
        return texts;
    }

    @SuppressWarnings("unchecked")
    private void createInferenceListForMapTypeInput(Object sourceValue, List<String> texts) {
        if (sourceValue instanceof Map) {
            ((Map<String, Object>) sourceValue).forEach((k, v) -> createInferenceListForMapTypeInput(v, texts));
        } else if (sourceValue instanceof List) {
            texts.addAll(((List<String>) sourceValue));
        } else {
            if (sourceValue == null) return;
            texts.add(sourceValue.toString());
        }
    }

    private void setTargetFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap, List<?> results) {
        Objects.requireNonNull(results, "embedding failed, inference returns null result!");
        log.debug("Model inference result fetched, starting build vector output!");
        Map<String, Object> result = buildResult(processorMap, results, ingestDocument.getSourceAndMetadata());
        result.forEach(ingestDocument::setFieldValue);
    }

    @VisibleForTesting
    Map<String, Object> buildResult(Map<String, Object> processorMap, List<?> results, Map<String, Object> sourceAndMetadataMap) {
        InferenceProcessor.IndexWrapper indexWrapper = new InferenceProcessor.IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : processorMap.entrySet()) {
            String knnKey = knnMapEntry.getKey();
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof String) {
                result.put(knnKey, results.get(indexWrapper.index++));
            } else if (sourceValue instanceof List) {
                result.put(knnKey, buildResultForListType((List<Object>) sourceValue, results, indexWrapper));
            } else if (sourceValue instanceof Map) {
                putResultToSourceMapForMapType(knnKey, sourceValue, results, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void putResultToSourceMapForMapType(
        String processorKey,
        Object sourceValue,
        List<?> results,
        InferenceProcessor.IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (processorKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                if (sourceAndMetadataMap.get(processorKey) instanceof List) {
                    // build output for list of nested objects
                    for (Map<String, Object> nestedElement : (List<Map<String, Object>>) sourceAndMetadataMap.get(processorKey)) {
                        nestedElement.put(inputNestedMapEntry.getKey(), results.get(indexWrapper.index++));
                    }
                } else {
                    putResultToSourceMapForMapType(
                        inputNestedMapEntry.getKey(),
                        inputNestedMapEntry.getValue(),
                        results,
                        indexWrapper,
                        (Map<String, Object>) sourceAndMetadataMap.get(processorKey)
                    );
                }
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.put(processorKey, results.get(indexWrapper.index++));
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.put(processorKey, buildResultForListType((List<Object>) sourceValue, results, indexWrapper));
        }
    }

    public static class Factory implements Processor.Factory {

        private final Settings settings;

        private final ClusterService clusterService;

        private final IndicesService indicesService;

        private final AnalysisRegistry analysisRegistry;

        public Factory(Settings settings, ClusterService clusterService, IndicesService indicesService, AnalysisRegistry analysisRegistry) {
            this.settings = settings;
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
                settings,
                clusterService,
                indicesService,
                analysisRegistry
            );
        }

    }
}
