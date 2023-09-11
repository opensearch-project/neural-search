/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;


@Log4j2
public class SparseEncodingProcessor extends NLPProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "sparseEncoding";

    public SparseEncodingProcessor(String tag, String description, String modelId, Map<String, Object> fieldMap, MLCommonsClientAccessor clientAccessor, Environment environment) {
        super(tag, description, modelId, fieldMap, clientAccessor, environment);
    }

    @Override
    public void doExecute(IngestDocument ingestDocument, Map<String, Object> ProcessMap, List<String> inferenceList, BiConsumer<IngestDocument, Exception> handler) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            List<Map<String, ?> > resultTokenWeights = new ArrayList<>();
            for (Map<String, ?> map: resultMaps)
            {
                resultTokenWeights.addAll((List<Map<String, ?>>)map.get("response") );
            }
            log.info(resultTokenWeights);
            setVectorFieldsToDocument(ingestDocument, ProcessMap, resultTokenWeights);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }


    void setVectorFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap, List<Map<String, ?> > resultTokenWeights) {
        Objects.requireNonNull(resultTokenWeights, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> sparseEncodingResult = buildSparseEncodingResult(processorMap, resultTokenWeights, ingestDocument.getSourceAndMetadata());
        sparseEncodingResult.forEach(ingestDocument::setFieldValue);
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

    @SuppressWarnings({ "unchecked" })
    private void buildMapWithProcessorKeyAndOriginalValueForMapType(
            String parentKey,
            Object processorKey,
            Map<String, Object> sourceAndMetadataMap,
            Map<String, Object> treeRes
    ) {
        if (processorKey == null || sourceAndMetadataMap == null) return;
        if (processorKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                        next
                );
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(processorKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildSparseEncodingResult(
            Map<String, Object> processorMap,
            List<Map<String, ?> > resultTokenWeights,
            Map<String, Object> sourceAndMetadataMap
    ) {
        SparseEncodingProcessor.IndexWrapper indexWrapper = new SparseEncodingProcessor.IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : processorMap.entrySet()) {
            String knnKey = knnMapEntry.getKey();
            Object sourceValue = knnMapEntry.getValue();
            if (sourceValue instanceof String) {
                result.put(knnKey, resultTokenWeights.get(indexWrapper.index++));
            } else if (sourceValue instanceof List) {
                result.put(knnKey, buildSparseEncodingResultForListType((List<String>) sourceValue, resultTokenWeights, indexWrapper));
            } else if (sourceValue instanceof Map) {
                putSparseEncodingResultToSourceMapForMapType(knnKey, sourceValue, resultTokenWeights, indexWrapper, sourceAndMetadataMap);
            }
        }
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void putSparseEncodingResultToSourceMapForMapType(
            String processorKey,
            Object sourceValue,
            List<Map<String, ?> > resultTokenWeights,
            SparseEncodingProcessor.IndexWrapper indexWrapper,
            Map<String, Object> sourceAndMetadataMap
    ) {
        if (processorKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                putSparseEncodingResultToSourceMapForMapType(
                        inputNestedMapEntry.getKey(),
                        inputNestedMapEntry.getValue(),
                        resultTokenWeights,
                        indexWrapper,
                        (Map<String, Object>) sourceAndMetadataMap.get(processorKey)
                );
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.put(processorKey, resultTokenWeights.get(indexWrapper.index++));
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.put(
                    processorKey,
                    buildSparseEncodingResultForListType((List<String>) sourceValue, resultTokenWeights, indexWrapper)
            );
        }
    }

    private List<Map<String, Map<String, ?>>> buildSparseEncodingResultForListType(
            List<String> sourceValue,
            List<Map<String, ?> > resultTokenWeights,
            SparseEncodingProcessor.IndexWrapper indexWrapper
    ) {
        List<Map<String, Map<String, ?>>> tokenWeights = new ArrayList<>();
        IntStream.range(0, sourceValue.size())
                .forEachOrdered(x -> tokenWeights.add(ImmutableMap.of(LIST_TYPE_NESTED_MAP_KEY, resultTokenWeights.get(indexWrapper.index++))));
        return tokenWeights;
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
