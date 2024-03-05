/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;

/**
 * The abstract class for text processing use cases. Users provide a field name map and a model id.
 * During ingestion, the processor will use the corresponding model to inference the input texts,
 * and set the target fields according to the field name map.
 */
@Log4j2
public abstract class InferenceProcessor extends AbstractProcessor {

    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private final String type;

    // This field is used for nested knn_vector/rank_features field. The value of the field will be used as the
    // default key for the nested object.
    private final String listTypeNestedMapKey;

    protected final String modelId;

    protected final Map<String, Object> fieldMap;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    protected final Environment environment;

    protected final ProcessorInputValidator processorInputValidator;

    public InferenceProcessor(
        String tag,
        String description,
        String type,
        String listTypeNestedMapKey,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ProcessorInputValidator processorInputValidator
    ) {
        super(tag, description);
        this.type = type;
        validateEmbeddingConfiguration(fieldMap);

        this.listTypeNestedMapKey = listTypeNestedMapKey;
        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
        this.processorInputValidator = processorInputValidator;
    }

    private void validateEmbeddingConfiguration(Map<String, Object> fieldMap) {
        if (fieldMap == null
            || fieldMap.size() == 0
            || fieldMap.entrySet()
                .stream()
                .anyMatch(
                    x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()) || StringUtils.isBlank(x.getValue().toString())
                )) {
            throw new IllegalArgumentException("Unable to create the processor as field_map has invalid key or value");
        }
    }

    public abstract void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    );

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        return ingestDocument;
    }

    /**
     * This method will be invoked by PipelineService to make async inference and then delegate the handler to
     * process the inference response or failure.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     * @param handler {@link BiConsumer} which is the handler which can be used after the inference task is done.
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            processorInputValidator.validateFieldsValue(fieldMap, environment, ingestDocument, false);
            Map<String, Object> processMap = buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
            List<String> inferenceList = createInferenceList(processMap);
            if (inferenceList.isEmpty()) {
                handler.accept(ingestDocument, null);
            } else {
                doExecute(ingestDocument, processMap, inferenceList, handler);
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    protected List<String> createInferenceList(Map<String, Object> knnKeyMap) {
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

    @VisibleForTesting
    Map<String, Object> buildMapWithProcessorKeyAndOriginalValue(IngestDocument ingestDocument) {
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

    protected void setTargetFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap, List<?> results) {
        Objects.requireNonNull(results, "embedding failed, inference returns null result!");
        log.debug("Model inference result fetched, starting build vector output!");
        Map<String, Object> result = buildResult(processorMap, results, ingestDocument.getSourceAndMetadata());
        result.forEach(ingestDocument::setFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildResult(Map<String, Object> processorMap, List<?> results, Map<String, Object> sourceAndMetadataMap) {
        IndexWrapper indexWrapper = new IndexWrapper(0);
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
        IndexWrapper indexWrapper,
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

    protected List<?> buildResultForListType(List<Object> sourceValue, List<?> results, IndexWrapper indexWrapper) {
        Object peek = sourceValue.get(0);
        if (peek instanceof String) {
            List<Map<String, Object>> keyToResult = new ArrayList<>();
            IntStream.range(0, sourceValue.size())
                .forEachOrdered(x -> keyToResult.add(ImmutableMap.of(listTypeNestedMapKey, results.get(indexWrapper.index++))));
            return keyToResult;
        } else {
            List<List<Map<String, Object>>> keyToResult = new ArrayList<>();
            for (Object nestedList : sourceValue) {
                List<Map<String, Object>> nestedResult = new ArrayList<>();
                IntStream.range(0, ((List) nestedList).size())
                    .forEachOrdered(x -> nestedResult.add(ImmutableMap.of(listTypeNestedMapKey, results.get(indexWrapper.index++))));
                keyToResult.add(nestedResult);
            }
            return keyToResult;
        }
    }

    @Override
    public String getType() {
        return type;
    }

    /**
     * Since we need to build a {@link List<String>} as the input for text embedding, and the result type is {@link List<Float>} of {@link List},
     * we need to map the result back to the input one by one with exactly order. For nested map type input, we're performing a pre-order
     * traversal to extract the input strings, so when mapping back to the nested map, we still need a pre-order traversal to ensure the
     * order. And we also need to ensure the index pointer goes forward in the recursive, so here the IndexWrapper is to store and increase
     * the index pointer during the recursive.
     * index: the index pointer of the text embedding result.
     */
    static class IndexWrapper {
        protected int index;

        protected IndexWrapper(int index) {
            this.index = index;
        }
    }
}
