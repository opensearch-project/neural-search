/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.*;
import java.util.concurrent.ExecutionException;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * Before querying with neural search, all the documents should be ingested in the form of embedded vectors. Leaving the embedding process
 * to the user offline will raise the bar of use, thus we create this processor for user input document text embedding.
 */
@Log4j2
public class TextEmbeddingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "knn";

    private final String modelId;

    private final Map<String, Object> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;

    public TextEmbeddingProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null, can not process it");
        if (fieldMap == null || fieldMap.size() == 0 || checkEmbeddingConfigNotValid(fieldMap)) throw new IllegalArgumentException(
            "Unable to create the TextEmbedding processor as field_map is null or empty."
        );

        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
    }

    private boolean checkEmbeddingConfigNotValid(Map<String, Object> fieldMap) {
        return fieldMap.entrySet()
            .stream()
            .anyMatch(x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()) || StringUtils.isBlank(x.getValue().toString()));
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        validateEmbeddingFieldsType(ingestDocument, fieldMap);
        Map<String, Object> knnMap = buildKnnMap(ingestDocument, fieldMap);
        try {
            List<List<Float>> vectors = mlCommonsClientAccessor.blockingInferenceSentences(this.modelId, createInferenceList(knnMap));
            processResponse(ingestDocument, knnMap, vectors);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Text embedding processor failed with exception: " + e.getMessage(), e);
            throw new RuntimeException("Text embedding processor failed with exception", e);
        }
        log.info("Text embedding completed, returning ingestDocument!");
        return ingestDocument;
    }

    void processResponse(IngestDocument ingestDocument, Map<String, Object> knnMap, List<List<Float>> vectors) {
        Objects.requireNonNull(vectors, "embedding failed, inference returns null result!");
        log.info("Text embedding result fetched, starting build vector output!");
        Map<String, Object> vectorOutput = buildVectorOutput(knnMap, vectors, ingestDocument.getSourceAndMetadata());
        vectorOutput.forEach(ingestDocument::appendFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> createInferenceList(Map<String, Object> knnMap) {
        List<String> texts = new LinkedList<>();
        knnMap.entrySet().stream().filter(knnMapEntry -> knnMapEntry.getValue() != null).forEach(knnMapEntry -> {
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof List) {
                ((List<String>) sourceValue).stream().filter(StringUtils::isNotBlank).forEach(texts::add);
            } else if (sourceValue instanceof Map) {
                processMapTypeInput(sourceValue, texts);
            } else {
                texts.add(sourceValue.toString());
            }
        });
        return texts;
    }

    @SuppressWarnings("unchecked")
    private void processMapTypeInput(Object sourceValue, List<String> texts) {
        if (sourceValue instanceof Map) {
            ((Map<String, Object>) sourceValue).forEach((k, v) -> processMapTypeInput(v, texts));
        } else if (sourceValue instanceof List) {
            ((List<String>) sourceValue).stream().filter(StringUtils::isNotBlank).forEach(texts::add);
        } else {
            if (sourceValue == null || StringUtils.isBlank(sourceValue.toString())) return;
            texts.add(sourceValue.toString());
        }
    }

    @VisibleForTesting
    Map<String, Object> buildKnnMap(IngestDocument ingestDocument, Map<String, Object> fieldMap) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> knnMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                processConfiguredMapType(originalKey, targetKey, sourceAndMetadataMap, treeRes);
                knnMap.put(originalKey, treeRes.get(originalKey));
            } else {
                knnMap.put(String.valueOf(targetKey), sourceAndMetadataMap.get(originalKey));
            }
        }
        return knnMap;
    }

    @SuppressWarnings({ "unchecked" })
    private void processConfiguredMapType(
        String parentKey,
        Object knnKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> treeRes
    ) {
        if (knnKey == null || sourceAndMetadataMap == null) return;
        if (knnKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) knnKey).entrySet()) {
                processConfiguredMapType(
                    nestedFieldMapEntry.getKey(),
                    nestedFieldMapEntry.getValue(),
                    (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                    next
                );
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(knnKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildVectorOutput(
        Map<String, Object> knnMap,
        List<List<Float>> modelTensorList,
        Map<String, Object> sourceAndMetadataMap
    ) {
        IndexWrapper indexWrapper = new IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : knnMap.entrySet()) {
            String knnKey = knnMapEntry.getKey();
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof String && StringUtils.isNotBlank(sourceValue.toString())) {
                List<Float> modelTensor = modelTensorList.get(indexWrapper.index++);
                result.put(knnKey, modelTensor);
            } else if (sourceValue instanceof List) {
                result.put(knnKey, processListOut((List<String>) sourceValue, modelTensorList, indexWrapper));
            } else if (sourceValue instanceof Map) {
                processMapTypeVectorOutput(knnKey, sourceValue, modelTensorList, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void processMapTypeVectorOutput(
        String knnKey,
        Object sourceValue,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (knnKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                processMapTypeVectorOutput(
                    inputNestedMapEntry.getKey(),
                    inputNestedMapEntry.getValue(),
                    modelTensorList,
                    indexWrapper,
                    (Map<String, Object>) sourceAndMetadataMap.get(knnKey)
                );
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.put(knnKey, modelTensorList.get(indexWrapper.index++));
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.put(knnKey, processListOut((List<String>) sourceValue, modelTensorList, indexWrapper));
        }
    }

    private List<Map<String, List<Float>>> processListOut(
        List<String> sourceValue,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper
    ) {
        List<Map<String, List<Float>>> numbers = new ArrayList<>();
        for (String strSourceValue : sourceValue) {
            if (StringUtils.isNotBlank(strSourceValue)) {
                numbers.add(ImmutableMap.of(LIST_TYPE_NESTED_MAP_KEY, modelTensorList.get(indexWrapper.index++)));
            }
        }
        return numbers;
    }

    private static void validateEmbeddingFieldsType(IngestDocument ingestDocument, Map<String, Object> embeddingFields) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> embeddingFieldsEntry : embeddingFields.entrySet()) {
            Object sourceValue = sourceAndMetadataMap.get(embeddingFieldsEntry.getKey());
            if (sourceValue != null) {
                String sourceKey = embeddingFieldsEntry.getKey();
                Class<?> sourceValueClass = sourceValue.getClass();
                if (List.class.isAssignableFrom(sourceValueClass) || Map.class.isAssignableFrom(sourceValueClass)) {
                    checkListElementsType(sourceKey, sourceValue);
                } else if (!String.class.isAssignableFrom(sourceValueClass)) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] is neither string nor nested type, can not process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void checkListElementsType(String sourceKey, Object sourceValue) {
        if (List.class.isAssignableFrom(sourceValue.getClass())) {
            ((List) sourceValue).stream().filter(Objects::nonNull).forEach(x -> checkListElementsType(sourceKey, x));
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values().stream().filter(Objects::nonNull).forEach(x -> checkListElementsType(sourceKey, x));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException("nested type field [" + sourceKey + "] has non-string type, can not process it");
        }
    }

    @Override
    public String getType() {
        return TYPE;
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
        private int index;

        protected IndexWrapper(int index) {
            this.index = index;
        }
    }

}
