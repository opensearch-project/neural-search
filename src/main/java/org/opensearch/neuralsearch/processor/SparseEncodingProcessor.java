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
public class SparseEncodingProcessor extends AbstractProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "sparseEncoding";

    @VisibleForTesting
    private final String modelId;

    private final Map<String, Object> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;

    private final Environment environment;

    public SparseEncodingProcessor(
            String tag,
            String description,
            String modelId,
            Map<String, Object> fieldMap,
            MLCommonsClientAccessor clientAccessor,
            Environment environment
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, can not process it");
        validateEmbeddingConfiguration(fieldMap);

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
                        x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()) || StringUtils.isBlank(x.getValue().toString())
                )) {
            throw new IllegalArgumentException("Unable to create the TextEmbedding processor as field_map has invalid key or value");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
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
        // When received a bulk indexing request, the pipeline will be executed in this method, (see
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/bulk/TransportBulkAction.java#L226).
        // Before the pipeline execution, the pipeline will be marked as resolved (means executed),
        // and then this overriding method will be invoked when executing the text embedding processor.
        // After the inference completes, the handler will invoke the doInternalExecute method again to run actual write operation.
        try {
            validateEmbeddingFieldsValue(ingestDocument);
            Map<String, Object> ProcessMap = buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
            List<String> inferenceList = createInferenceList(ProcessMap);
            if (inferenceList.size() == 0) {
                handler.accept(ingestDocument, null);
            } else {
                mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMap -> {
                    Map.Entry<String, List<Map<String, Float>>> entry = ((Map<String, List<Map<String, Float>>>)resultMap).entrySet().iterator().next();
                    List<Map<String, Float> > resultTokenWeights = entry.getValue();
                    setVectorFieldsToDocument(ingestDocument, ProcessMap, resultTokenWeights);
                    handler.accept(ingestDocument, null);
                }, e -> { handler.accept(null, e); }));
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }

    }

    void setVectorFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap, List<Map<String, Float> > resultTokenWeights) {
        Objects.requireNonNull(resultTokenWeights, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> sparseEncodingResult = buildSparseEncodingResult(processorMap, resultTokenWeights, ingestDocument.getSourceAndMetadata());
        sparseEncodingResult.forEach(ingestDocument::setFieldValue);
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
            List<Map<String, Float> > resultTokenWeights,
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
            List<Map<String, Float> > resultTokenWeights,
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

    private List<Map<String, Map<String, Float>>> buildSparseEncodingResultForListType(
            List<String> sourceValue,
            List<Map<String, Float> > resultTokenWeights,
            SparseEncodingProcessor.IndexWrapper indexWrapper
    ) {
        List<Map<String, Map<String, Float>>> tokenWeights = new ArrayList<>();
        IntStream.range(0, sourceValue.size())
                .forEachOrdered(x -> tokenWeights.add(ImmutableMap.of(LIST_TYPE_NESTED_MAP_KEY, resultTokenWeights.get(indexWrapper.index++))));
        return tokenWeights;
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
                    throw new IllegalArgumentException("field [" + sourceKey + "] is neither string nor nested type, can not process it");
                } else if (StringUtils.isBlank(sourceValue.toString())) {
                    throw new IllegalArgumentException("field [" + sourceKey + "] has empty string value, can not process it");
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void validateNestedTypeValue(String sourceKey, Object sourceValue, Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(environment.settings())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] reached max depth limit, can not process it");
        } else if ((List.class.isAssignableFrom(sourceValue.getClass()))) {
            validateListTypeValue(sourceKey, sourceValue);
        } else if (Map.class.isAssignableFrom(sourceValue.getClass())) {
            ((Map) sourceValue).values()
                    .stream()
                    .filter(Objects::nonNull)
                    .forEach(x -> validateNestedTypeValue(sourceKey, x, () -> maxDepth + 1));
        } else if (!String.class.isAssignableFrom(sourceValue.getClass())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has non-string type, can not process it");
        } else if (StringUtils.isBlank(sourceValue.toString())) {
            throw new IllegalArgumentException("map type field [" + sourceKey + "] has empty string, can not process it");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private static void validateListTypeValue(String sourceKey, Object sourceValue) {
        for (Object value : (List) sourceValue) {
            if (value == null) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has null, can not process it");
            } else if (!(value instanceof String)) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has non string value, can not process it");
            } else if (StringUtils.isBlank(value.toString())) {
                throw new IllegalArgumentException("list type field [" + sourceKey + "] has empty string, can not process it");
            }
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
