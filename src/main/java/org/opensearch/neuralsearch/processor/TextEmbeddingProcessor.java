/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.*;
import java.util.function.Consumer;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

@Log4j2
public class TextEmbeddingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "knn";

    private final String modelId;

    private final Map<String, Object> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;

    public TextEmbeddingProcessor(String tag, String description, String modelId, Map<String, Object> fieldMap, Client client) {
        super(tag, description);
        this.modelId = Objects.requireNonNull(modelId, "model_id is null, can not process it");
        if (fieldMap == null || fieldMap.size() == 0 || checkEmbeddingConfigNotValid(fieldMap)) {
            throw new IllegalArgumentException("filed_map is null, can not process it");
        } else {
            this.fieldMap = fieldMap;
        }
        this.mlCommonsClientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(client));
    }

    private boolean checkEmbeddingConfigNotValid(Map<String, Object> fieldMap) {
        return fieldMap.entrySet().stream().anyMatch(x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()));
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        validateEmbeddingFieldsType(ingestDocument, fieldMap);
        Map<String, Object> knnMap = buildKnnMap(ingestDocument, fieldMap);

        ActionListener<List<List<Float>>> internalListener = ActionListener.wrap(
            responseConsumer(ingestDocument, knnMap),
            exceptionConsumer()
        );

        mlCommonsClientAccessor.inferenceSentences(this.modelId, createInferenceList(knnMap), internalListener);
        return ingestDocument;
    }

    @VisibleForTesting
    CheckedConsumer<List<List<Float>>, Exception> responseConsumer(IngestDocument ingestDocument, Map<String, Object> knnMap) {
        return res -> {
            Objects.requireNonNull(res, "embedding failed, inference returns null result!");
            Map<String, Object> vectorOutput = buildVectorOutput(knnMap, res, ingestDocument.getSourceAndMetadata());
            vectorOutput.forEach(ingestDocument::appendFieldValue);
        };
    }

    @VisibleForTesting
    Consumer<Exception> exceptionConsumer() {
        return exception -> log.error("Text embedding processor failed with exception: " + exception.getMessage(), exception);
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> createInferenceList(Map<String, Object> knnMap) {
        List<String> texts = new LinkedList<>();
        knnMap.entrySet().stream().filter(entry -> entry.getValue() != null).forEach(entry -> {
            Object value = entry.getValue();
            if (value instanceof List) {
                ((List<String>) value).stream().filter(StringUtils::isNotBlank).forEach(texts::add);
            } else if (value instanceof Map) {
                processMapTypeInput(value, texts);
            } else {
                texts.add(value.toString());
            }
        });
        return texts;
    }

    @SuppressWarnings("unchecked")
    private void processMapTypeInput(Object value, List<String> texts) {
        if (value instanceof Map) {
            ((Map<String, Object>) value).forEach((k, v) -> processMapTypeInput(v, texts));
        } else if (value instanceof List) {
            ((List<String>) value).stream().filter(StringUtils::isNotBlank).forEach(texts::add);
        } else {
            if (value == null || StringUtils.isBlank(value.toString())) return;
            texts.add(value.toString());
        }
    }

    @VisibleForTesting
    Map<String, Object> buildKnnMap(IngestDocument ingestDocument, Map<String, Object> fieldMap) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> knnMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            String originalKey = entry.getKey();
            Object targetKey = entry.getValue();
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
        Object targetKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> treeRes
    ) {
        if (targetKey == null || sourceAndMetadataMap == null) return;
        if (targetKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) targetKey).entrySet()) {
                processConfiguredMapType(entry.getKey(), entry.getValue(), (Map<String, Object>) sourceAndMetadataMap.get(parentKey), next);
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(targetKey);
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
        for (Map.Entry<String, Object> entry : knnMap.entrySet()) {
            String targetKey = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String && StringUtils.isNotBlank(value.toString())) {
                List<Float> modelTensor = modelTensorList.get(indexWrapper.index++);
                result.put(targetKey, modelTensor);
            } else if (value instanceof List) {
                result.put(targetKey, processListOut((List<String>) value, modelTensorList, indexWrapper));
            } else if (value instanceof Map) {
                processMapTypeVectorOutput(targetKey, value, modelTensorList, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void processMapTypeVectorOutput(
        String targetKey,
        Object value,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (targetKey == null || sourceAndMetadataMap == null || value == null) return;
        if (value instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                processMapTypeVectorOutput(
                    entry.getKey(),
                    entry.getValue(),
                    modelTensorList,
                    indexWrapper,
                    (Map<String, Object>) sourceAndMetadataMap.get(String.valueOf(targetKey))
                );
            }
        } else if (value instanceof String) {
            sourceAndMetadataMap.put(targetKey, modelTensorList.get(indexWrapper.index++));
        } else if (value instanceof List) {
            sourceAndMetadataMap.put(targetKey, processListOut((List<String>) value, modelTensorList, indexWrapper));
        }
    }

    private List<Map<String, List<Float>>> processListOut(
        List<String> value,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper
    ) {
        List<Map<String, List<Float>>> numbers = new ArrayList<>();
        for (String strValue : value) {
            if (StringUtils.isNotBlank(strValue)) {
                numbers.add(ImmutableMap.of(LIST_TYPE_NESTED_MAP_KEY, modelTensorList.get(indexWrapper.index++)));
            }
        }
        return numbers;
    }

    private static void validateEmbeddingFieldsType(IngestDocument ingestDocument, Map<String, Object> embeddingFields) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        for (Map.Entry<String, Object> entry : embeddingFields.entrySet()) {
            Object obj = sourceAndMetadataMap.get(entry.getKey());
            if (obj != null) {
                String key = entry.getKey();
                Class<?> clz = obj.getClass();
                if (List.class.isAssignableFrom(clz) || Map.class.isAssignableFrom(clz)) {
                    checkListElementsType(obj, key);
                } else if (!String.class.isAssignableFrom(clz)) {
                    throw new IllegalArgumentException("field [" + key + "] is neither string nor nested type, can not process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void checkListElementsType(Object obj, String key) {
        if (List.class.isAssignableFrom(obj.getClass())) {
            ((List) obj).stream().filter(Objects::nonNull).forEach(x -> checkListElementsType(x, key));
        } else if (Map.class.isAssignableFrom(obj.getClass())) {
            ((Map) obj).values().stream().filter(Objects::nonNull).forEach(x -> checkListElementsType(x, key));
        } else if (!String.class.isAssignableFrom(obj.getClass())) {
            throw new IllegalArgumentException("nested type field [" + key + "] has non-string type, can not process it");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final Client client;

        public Factory(Client client) {
            this.client = client;
        }

        @Override
        public TextEmbeddingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String modelId = readStringProperty(TYPE, processorTag, config, MODEL_ID_FIELD);
            Map<String, Object> filedMap = readOptionalMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
            return new TextEmbeddingProcessor(processorTag, description, modelId, filedMap, client);
        }
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
