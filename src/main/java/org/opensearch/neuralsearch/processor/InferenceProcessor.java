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
import java.util.function.Supplier;
import java.util.stream.IntStream;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * The abstract class for text processing use cases. Users provide a field name
 * map and a model id.
 * During ingestion, the processor will use the corresponding model to inference
 * the input texts,
 * and set the target fields according to the field name map.
 */
@Log4j2
public abstract class InferenceProcessor extends AbstractProcessor {

    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private final String type;

    // This field is used for nested knn_vector/rank_features field. The value of
    // the field will be used as the
    // default key for the nested object.
    private final String listTypeNestedMapKey;

    protected final String modelId;

    private final Map<String, Object> fieldMap;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    private final Environment environment;

    public InferenceProcessor(
            String tag,
            String description,
            String type,
            String listTypeNestedMapKey,
            String modelId,
            Map<String, Object> fieldMap,
            MLCommonsClientAccessor clientAccessor,
            Environment environment) {
        super(tag, description);
        this.type = type;
        if (StringUtils.isBlank(modelId))
            throw new IllegalArgumentException("model_id is null or empty, cannot process it");
        validateEmbeddingConfiguration(fieldMap);

        this.listTypeNestedMapKey = listTypeNestedMapKey;
        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
    }

    private void validateEmbeddingConfiguration(Map<String, Object> fieldMap) {
        if (fieldMap == null
                || fieldMap.size() == 0
                || fieldMap.entrySet()
                        .stream()
                        .anyMatch(
                                x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue())
                                        || StringUtils.isBlank(x.getValue().toString()))) {
            throw new IllegalArgumentException("Unable to create the processor as field_map has invalid key or value");
        }
    }

    public abstract void doExecute(
            IngestDocument ingestDocument,
            Map<String, Object> ProcessMap,
            List<String> inferenceList,
            BiConsumer<IngestDocument, Exception> handler);

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        return ingestDocument;
    }

    /**
     * This method will be invoked by PipelineService to make async inference and
     * then delegate the handler to
     * process the inference response or failure.
     * 
     * @param ingestDocument {@link IngestDocument} which is the document passed to
     *                       processor.
     * @param handler        {@link BiConsumer} which is the handler which can be
     *                       used after the inference task is done.
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            validateEmbeddingFieldsValue(ingestDocument);
            Map<String, Object> ProcessMap = buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
            List<String> inferenceList = createInferenceList(ProcessMap);
            if (inferenceList.size() == 0) {
                handler.accept(ingestDocument, null);
            } else {
                doExecute(ingestDocument, ProcessMap, inferenceList, handler);
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> createInferenceList(Map<String, Object> knnKeyMap) {
        List<String> texts = new ArrayList<>();
        knnKeyMap.entrySet().stream().filter(knnMapEntry -> knnMapEntry.getValue() != null).forEach(knnMapEntry -> {
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof List) {
                texts.addAll(((List<String>) sourceValue));
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
            if (sourceValue == null)
                return;
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

            int nestedDotIndex = originalKey.indexOf('.');
            if (nestedDotIndex != -1) {
                Map<String, Object> newTargetKey = new LinkedHashMap<>();
                newTargetKey.put(originalKey.substring(nestedDotIndex + 1), targetKey);
                targetKey = newTargetKey;

                originalKey = originalKey.substring(0, nestedDotIndex);
            }

            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildMapWithProcessorKeyAndOriginalValueForMapType(originalKey, targetKey, sourceAndMetadataMap,
                        treeRes);
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
            Map<String, Object> treeRes) {
        if (processorKey == null || sourceAndMetadataMap == null)
            return;
        if (processorKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                        next);
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(processorKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    private void validateEmbeddingFieldsValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> embeddingFieldsEntry : fieldMap.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
                String sourceKey = embeddingFieldsEntry.getKey();
                Class<?> sourceValueClass = sourceValue.getClass();
                if (List.class.isAssignableFrom(sourceValueClass) || Map.class.isAssignableFrom(sourceValueClass)) {
                    validateNestedTypeValue(sourceKey, sourceValue, () -> 1);
                } else if (!String.class.isAssignableFrom(sourceValueClass)) {
                    throw new IllegalArgumentException(
                            "field [" + sourceKey + "] is neither string nor nested type, cannot process it");
                } else if (StringUtils.isBlank(sourceValue.toString())) {
                    throw new IllegalArgumentException(
                            "field [" + sourceKey + "] has empty string value, cannot process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(String sourceKey, Object sourceValue, Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException(
                    "map type field [" + sourceKey + "] reached max depth limit, cannot process it");
        } else if ((List.class.isAssignableFrom(sourceValue.getClass()))) {
            validateListTypeValue(sourceKey, sourceValue);
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values()
                    .stream()
                    .filter(Objects::nonNull)
                    .forEach(x -> validateNestedTypeValue(sourceKey, x, () -> maxDepth + 1));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException(
                    "map type field [" + sourceKey + "] has non-string type, cannot process it");
        } else if (StringUtils.isBlank(sourceValue.toString())) {
            throw new IllegalArgumentException(
                    "map type field [" + sourceKey + "] has empty string, cannot process it");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void validateListTypeValue(String sourceKey, Object sourceValue) {
        for (Object value : (List) sourceValue) {
            if (value == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, cannot process it");
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException(
                        "list type field [" + sourceKey + "] has non string value, cannot process it");
            } else if (StringUtils.isBlank(value.toString())) {
                throw new IllegalArgumentException(
                        "list type field [" + sourceKey + "] has empty string, cannot process it");
            }
        }
    }

    protected void setVectorFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap,
            List<?> results) {
        Objects.requireNonNull(results, "embedding failed, inference returns null result!");
        log.debug("Model inference result fetched, starting build vector output!");
        Map<String, Object> nlpResult = buildNLPResult(processorMap, results, ingestDocument.getSourceAndMetadata());
        nlpResult.forEach(ingestDocument::setFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildNLPResult(Map<String, Object> processorMap, List<?> results,
            Map<String, Object> sourceAndMetadataMap) {
        IndexWrapper indexWrapper = new IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : processorMap.entrySet()) {
            String knnKey = knnMapEntry.getKey();
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof String) {
                result.put(knnKey, results.get(indexWrapper.index++));
            } else if (sourceValue instanceof List) {
                result.put(knnKey, buildNLPResultForListType((List<String>) sourceValue, results, indexWrapper));
            } else if (sourceValue instanceof Map) {
                putNLPResultToSourceMapForMapType(knnKey, sourceValue, results, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void putNLPResultToSourceMapForMapType(
            String processorKey,
            Object sourceValue,
            List<?> results,
            IndexWrapper indexWrapper,
            Map<String, Object> sourceAndMetadataMap) {
        if (processorKey == null || sourceAndMetadataMap == null || sourceValue == null)
            return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                putNLPResultToSourceMapForMapType(
                        inputNestedMapEntry.getKey(),
                        inputNestedMapEntry.getValue(),
                        results,
                        indexWrapper,
                        (Map<String, Object>) sourceAndMetadataMap.get(processorKey));
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.put(processorKey, results.get(indexWrapper.index++));
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.put(processorKey,
                    buildNLPResultForListType((List<String>) sourceValue, results, indexWrapper));
        }
    }

    private List<Map<String, Object>> buildNLPResultForListType(List<String> sourceValue, List<?> results,
            IndexWrapper indexWrapper) {
        List<Map<String, Object>> keyToResult = new ArrayList<>();
        IntStream.range(0, sourceValue.size())
                .forEachOrdered(
                        x -> keyToResult.add(ImmutableMap.of(listTypeNestedMapKey, results.get(indexWrapper.index++))));
        return keyToResult;
    }

    @Override
    public String getType() {
        return type;
    }

    /**
     * Since we need to build a {@link List<String>} as the input for text
     * embedding, and the result type is {@link List<Float>} of {@link List},
     * we need to map the result back to the input one by one with exactly order.
     * For nested map type input, we're performing a pre-order
     * traversal to extract the input strings, so when mapping back to the nested
     * map, we still need a pre-order traversal to ensure the
     * order. And we also need to ensure the index pointer goes forward in the
     * recursive, so here the IndexWrapper is to store and increase
     * the index pointer during the recursive.
     * index: the index pointer of the text embedding result.
     */
    static class IndexWrapper {
        private int index;

        protected IndexWrapper(int index) {
            this.index = index;
        }
    }
}
