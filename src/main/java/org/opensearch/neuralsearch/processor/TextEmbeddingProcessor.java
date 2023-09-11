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
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 */
@Log4j2
public class TextEmbeddingProcessor extends NLPProcessor {

    public static final String TYPE = "text_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "knn";


    public TextEmbeddingProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description, modelId, fieldMap, clientAccessor, environment);
    }

    private void validateEmbeddingConfiguration(Map<String, Object> fieldMap) {
        if (fieldMap == null
            || fieldMap.size() == 0
            || fieldMap.entrySet()
                .stream()
                .anyMatch(
                    x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()) || StringUtils.isBlank(x.getValue().toString())
                )) {
            throw new IllegalArgumentException("Unable to create the TextEmbedding processor as field_map has invalid key or value");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        return ingestDocument;
    }

    @Override
    public void doExecute(IngestDocument ingestDocument, Map<String, Object> ProcessMap, List<String> inferenceList, BiConsumer<IngestDocument, Exception> handler) {
        mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(vectors -> {
            setVectorFieldsToDocument(ingestDocument, ProcessMap, vectors);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }


    void setVectorFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> knnMap, List<List<Float>> vectors) {
        Objects.requireNonNull(vectors, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> textEmbeddingResult = buildTextEmbeddingResult(knnMap, vectors, ingestDocument.getSourceAndMetadata());
        textEmbeddingResult.forEach(ingestDocument::setFieldValue);
    }



    @VisibleForTesting
    Map<String, Object> buildMapWithKnnKeyAndOriginalValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> mapWithKnnKeys = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildMapWithKnnKeyAndOriginalValueForMapType(originalKey, targetKey, sourceAndMetadataMap, treeRes);
                mapWithKnnKeys.put(originalKey, treeRes.get(originalKey));
            } else {
                mapWithKnnKeys.put(String.valueOf(targetKey), sourceAndMetadataMap.get(originalKey));
            }
        }
        return mapWithKnnKeys;
    }

    @SuppressWarnings({ "unchecked" })
    private void buildMapWithKnnKeyAndOriginalValueForMapType(
        String parentKey,
        Object knnKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> treeRes
    ) {
        if (knnKey == null || sourceAndMetadataMap == null) return;
        if (knnKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) knnKey).entrySet()) {
                buildMapWithKnnKeyAndOriginalValueForMapType(
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
    Map<String, Object> buildTextEmbeddingResult(
        Map<String, Object> knnMap,
        List<List<Float>> modelTensorList,
        Map<String, Object> sourceAndMetadataMap
    ) {
        IndexWrapper indexWrapper = new IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : knnMap.entrySet()) {
            String knnKey = knnMapEntry.getKey();
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof String) {
                List<Float> modelTensor = modelTensorList.get(indexWrapper.index++);
                result.put(knnKey, modelTensor);
            } else if (sourceValue instanceof List) {
                result.put(knnKey, buildTextEmbeddingResultForListType((List<String>) sourceValue, modelTensorList, indexWrapper));
            } else if (sourceValue instanceof Map) {
                putTextEmbeddingResultToSourceMapForMapType(knnKey, sourceValue, modelTensorList, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void putTextEmbeddingResultToSourceMapForMapType(
        String knnKey,
        Object sourceValue,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (knnKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                putTextEmbeddingResultToSourceMapForMapType(
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
            sourceAndMetadataMap.put(
                knnKey,
                buildTextEmbeddingResultForListType((List<String>) sourceValue, modelTensorList, indexWrapper)
            );
        }
    }

    private List<Map<String, List<Float>>> buildTextEmbeddingResultForListType(
        List<String> sourceValue,
        List<List<Float>> modelTensorList,
        IndexWrapper indexWrapper
    ) {
        List<Map<String, List<Float>>> numbers = new ArrayList<>();
        IntStream.range(0, sourceValue.size())
            .forEachOrdered(x -> numbers.add(ImmutableMap.of(LIST_TYPE_NESTED_MAP_KEY, modelTensorList.get(indexWrapper.index++))));
        return numbers;
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
