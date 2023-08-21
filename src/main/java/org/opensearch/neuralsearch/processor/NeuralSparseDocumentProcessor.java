/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsNeuralSparseClientAccessor;

import com.google.common.annotations.VisibleForTesting;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 */
@Log4j2

public class NeuralSparseDocumentProcessor extends AbstractProcessor {
    public static final String TYPE = "neural_sparse";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String DEFAULT_EXPANSION_SURFIX = "expanded";

    @VisibleForTesting
    private final String modelId;

    private final Map<String, Object> fieldMap;

    private final MLCommonsNeuralSparseClientAccessor mlCommonsClientAccessor;

    private final Environment environment;

    public NeuralSparseDocumentProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsNeuralSparseClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, can not process it");
        // TODO validate expansion

        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
    }

    private void validateExpansionConfiguration(Map<String, Object> fiedlMap) {

    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        return ingestDocument;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            Map<String, Object> sparseMap = buildMapWithSparseKeyAndOriginalValue(ingestDocument);
            var inferenceList = createInferenceList(sparseMap);
            if (inferenceList.size() == 0) {
                handler.accept(ingestDocument, null);
            } else {
                mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(tw -> {
                    appendNeuralSparseResultToDocument(ingestDocument, sparseMap, tw);
                    handler.accept(ingestDocument, null);
                }, e -> { handler.accept(null, e); }));
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    void appendNeuralSparseResultToDocument(
        IngestDocument ingestDocument,
        Map<String, Object> expansionMap,
        List<Map<String, Double>> termWeightMapList
    ) {
        Objects.requireNonNull(termWeightMapList, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> neuralExpansionResult = buildNeuralSparseResult(
            expansionMap,
            termWeightMapList,
            ingestDocument.getSourceAndMetadata()
        );
        neuralExpansionResult.forEach(ingestDocument::setFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    private List<String> createInferenceList(Map<String, Object> sparseKeyMap) {
        List<String> texts = new ArrayList<>();
        sparseKeyMap.entrySet().stream().filter(sparseMapEntry -> sparseMapEntry.getValue() != null).forEach(sparseMapEntry -> {
            Object sourceValue = sparseMapEntry.getValue();
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
            if (sourceValue == null) return;
            texts.add(sourceValue.toString());
        }
    }

    @VisibleForTesting
    Map<String, Object> buildMapWithSparseKeyAndOriginalValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> mapWithSparseKeys = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildMapWithExpansionKeyAndOriginalValueForMapType(originalKey, targetKey, sourceAndMetadataMap, treeRes);
                mapWithSparseKeys.put(originalKey, treeRes.get(originalKey));
            } else {
                mapWithSparseKeys.put(String.valueOf(targetKey), sourceAndMetadataMap.get(originalKey));
            }
        }
        return mapWithSparseKeys;
    }

    @SuppressWarnings({ "unchecked" })
    private void buildMapWithExpansionKeyAndOriginalValueForMapType(
        String parentKey,
        Object expansionKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> treeRes
    ) {
        if (expansionKey == null || sourceAndMetadataMap == null) return;
        if (expansionKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) expansionKey).entrySet()) {
                buildMapWithExpansionKeyAndOriginalValueForMapType(
                    nestedFieldMapEntry.getKey(),
                    nestedFieldMapEntry.getValue(),
                    (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                    next
                );
            }
            treeRes.put(parentKey, next);
        } else {
            String key = String.valueOf(expansionKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildNeuralSparseResult(
        Map<String, Object> sparseMap,
        List<Map<String, Double>> termWeightMapList,
        Map<String, Object> sourceAndMetadataMap
    ) {
        log.info("building sparse result");
        IndexWrapper indexWrapper = new IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> sparseMapEntry : sparseMap.entrySet()) {
            String knnKey = sparseMapEntry.getKey();
            Object sourceValue = sparseMapEntry.getValue();
            log.info("key = " + knnKey + ", val = " + sourceValue.toString());
            if (sourceValue instanceof String) {
                log.info("i am here");
                var termWeights = termWeightMapList.get(indexWrapper.index++);
                result.put(knnKey, encodeTermWeightPairs(termWeights));
            } else if (sourceValue instanceof List) {
                result.put(knnKey, buildNeuralSparseResultForListType((List<String>) sourceValue, termWeightMapList, indexWrapper));
            } else if (sourceValue instanceof Map) {
                putNeuralSparseResultToSourceMapForMapType(knnKey, sourceValue, termWeightMapList, indexWrapper, sourceAndMetadataMap);
            }
        }
        log.info("built sparse result");
        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private void putNeuralSparseResultToSourceMapForMapType(
        String knnKey,
        Object sourceValue,
        List<Map<String, Double>> termWeightMapList,
        IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (knnKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                putNeuralSparseResultToSourceMapForMapType(
                    inputNestedMapEntry.getKey(),
                    inputNestedMapEntry.getValue(),
                    termWeightMapList,
                    indexWrapper,
                    (Map<String, Object>) sourceAndMetadataMap.get(knnKey)
                );
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.put(knnKey, encodeTermWeightPairs(termWeightMapList.get(indexWrapper.index++)));
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.put(
                knnKey,
                buildNeuralSparseResultForListType((List<String>) sourceValue, termWeightMapList, indexWrapper)
            );
        }
    }

    private List<String> buildNeuralSparseResultForListType(
        List<String> sourceValue,
        List<Map<String, Double>> termWeightMapList,
        IndexWrapper indexWrapper
    ) {
        List<String> twList = new ArrayList<>();
        IntStream.range(0, sourceValue.size())
            .forEachOrdered(x -> twList.add(encodeTermWeightPairs(termWeightMapList.get(indexWrapper.index++))));
        return twList;
    }

    private String encodeTermWeightPairs(Map<String, Double> termWeights) {
        log.info("termWeights = " + termWeights.toString());
        try{
            StringBuilder builder = new StringBuilder();
            for (var p : termWeights.entrySet()) {
                builder.append(p.getKey());
                builder.append("|");
                builder.append(p.getValue().toString());
                builder.append("|");
            }

            var tw = builder.substring(0, builder.length()-1);
            log.info("tw = " + tw);
            return tw;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.warn("Exception = " + e.toString() + ", stack = " + sw.toString());
        }

        return "";
    }

    static class IndexWrapper {
        private int index;

        protected IndexWrapper(int index) {
            this.index = index;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
