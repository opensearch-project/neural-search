/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;

import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.optimization.InferenceFilter;
import org.opensearch.neuralsearch.util.ProcessorDocumentUtils;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

/**
 * The abstract class for text processing use cases. Users provide a field name map and a model id.
 * During ingestion, the processor will use the corresponding model to inference the input texts,
 * and set the target fields according to the field name map.
 */
@Log4j2
public abstract class InferenceProcessor extends AbstractBatchingProcessor {

    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String INDEX_FIELD = "_index";
    public static final String ID_FIELD = "_id";
    public static final String SKIP_EXISTING = "skip_existing";
    public static final boolean DEFAULT_SKIP_EXISTING = false;
    private static final BiFunction<Object, Object, Object> REMAPPING_FUNCTION = (v1, v2) -> {
        if (v1 instanceof Collection && v2 instanceof Collection) {
            ((Collection) v1).addAll((Collection) v2);
            return v1;
        } else if (v1 instanceof Map && v2 instanceof Map) {
            ((Map) v1).putAll((Map) v2);
            return v1;
        } else {
            return v2;
        }
    };

    private final String type;

    // This field is used for nested knn_vector/rank_features field. The value of the field will be used as the
    // default key for the nested object.
    private final String listTypeNestedMapKey;

    protected final String modelId;

    private final Map<String, Object> fieldMap;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    private final Environment environment;
    private final ClusterService clusterService;

    public InferenceProcessor(
        String tag,
        String description,
        int batchSize,
        String type,
        String listTypeNestedMapKey,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize);
        this.type = type;
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, cannot process it");
        validateEmbeddingConfiguration(fieldMap);
        this.listTypeNestedMapKey = listTypeNestedMapKey;
        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
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
        Map<String, Object> processMap,
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
            preprocessIngestDocument(ingestDocument);
            validateEmbeddingFieldsValue(ingestDocument);
            Map<String, Object> processMap = buildMapWithTargetKeys(ingestDocument);
            List<String> inferenceList = createInferenceList(processMap);
            if (inferenceList.size() == 0) {
                handler.accept(ingestDocument, null);
            } else {
                doExecute(ingestDocument, processMap, inferenceList, handler);
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    @VisibleForTesting
    void preprocessIngestDocument(IngestDocument ingestDocument) {
        if (ingestDocument == null || ingestDocument.getSourceAndMetadata() == null) return;
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> unflattened = ProcessorDocumentUtils.unflattenJson(sourceAndMetadataMap);
        unflattened.forEach(ingestDocument::setFieldValue);
        sourceAndMetadataMap.keySet().removeIf(key -> key.contains("."));
    }

    /**
     * This is the function which does actual inference work for batchExecute interface.
     * @param inferenceList a list of String for inference.
     * @param handler a callback handler to handle inference results which is a list of objects.
     * @param onException an exception callback to handle exception.
     */
    abstract void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException);

    /**
     * This is the function which does actual inference work for subBatchExecute interface.
     * @param ingestDocumentWrappers a list of IngestDocuments in a batch.
     * @param handler a callback handler to handle inference results which is a list of objects.
     */
    @Override
    public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        try {
            if (CollectionUtils.isEmpty(ingestDocumentWrappers)) {
                handler.accept(ingestDocumentWrappers);
                return;
            }

            List<DataForInference> dataForInferences = getDataForInference(ingestDocumentWrappers);
            List<String> inferenceList = constructInferenceTexts(dataForInferences);
            if (inferenceList.isEmpty()) {
                handler.accept(ingestDocumentWrappers);
                return;
            }
            doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, handler);
        } catch (Exception e) {
            updateWithExceptions(ingestDocumentWrappers, handler, e);
        }
    }

    /**
     * This is a helper function for subBatchExecute, which invokes doBatchExecute for given inference list.
     * @param ingestDocumentWrappers a list of IngestDocuments in a batch.
     * @param inferenceList a list of String for inference.
     * @param dataForInferences a list of data for inference, which includes ingestDocumentWrapper, processMap, inferenceList.
     */
    protected void doSubBatchExecute(
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        List<String> inferenceList,
        List<DataForInference> dataForInferences,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        Tuple<List<String>, Map<Integer, Integer>> sortedResult = sortByLengthAndReturnOriginalOrder(inferenceList);
        inferenceList = sortedResult.v1();
        Map<Integer, Integer> originalOrder = sortedResult.v2();
        doBatchExecute(inferenceList, results -> {
            batchExecuteHandler(results, ingestDocumentWrappers, dataForInferences, originalOrder, handler);
            handler.accept(ingestDocumentWrappers);
        }, exception -> { updateWithExceptions(ingestDocumentWrappers, handler, exception); });
    }

    private void batchExecuteHandler(
        List<?> results,
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        List<DataForInference> dataForInferences,
        Map<Integer, Integer> originalOrder,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        int startIndex = 0;
        results = restoreToOriginalOrder(results, originalOrder);
        for (DataForInference dataForInference : dataForInferences) {
            if (dataForInference.getIngestDocumentWrapper().getException() != null
                || CollectionUtils.isEmpty(dataForInference.getInferenceList())) {
                continue;
            }
            List<?> inferenceResults = results.subList(startIndex, startIndex + dataForInference.getInferenceList().size());
            startIndex += dataForInference.getInferenceList().size();
            setVectorFieldsToDocument(
                dataForInference.getIngestDocumentWrapper().getIngestDocument(),
                dataForInference.getProcessMap(),
                inferenceResults
            );
        }
    }

    private Tuple<List<String>, Map<Integer, Integer>> sortByLengthAndReturnOriginalOrder(List<String> inferenceList) {
        List<Tuple<Integer, String>> docsWithIndex = new ArrayList<>();
        for (int i = 0; i < inferenceList.size(); ++i) {
            docsWithIndex.add(Tuple.tuple(i, inferenceList.get(i)));
        }
        docsWithIndex.sort(Comparator.comparingInt(t -> t.v2().length()));
        List<String> sortedInferenceList = docsWithIndex.stream().map(Tuple::v2).collect(Collectors.toList());
        Map<Integer, Integer> originalOrderMap = new HashMap<>();
        for (int i = 0; i < docsWithIndex.size(); ++i) {
            originalOrderMap.put(i, docsWithIndex.get(i).v1());
        }
        return Tuple.tuple(sortedInferenceList, originalOrderMap);
    }

    private List<?> restoreToOriginalOrder(List<?> results, Map<Integer, Integer> originalOrder) {
        List<Object> sortedResults = Arrays.asList(results.toArray());
        for (int i = 0; i < results.size(); ++i) {
            if (!originalOrder.containsKey(i)) continue;
            int oldIndex = originalOrder.get(i);
            sortedResults.set(oldIndex, results.get(i));
        }
        return sortedResults;
    }

    protected List<String> constructInferenceTexts(List<DataForInference> dataForInferences) {
        List<String> inferenceTexts = new ArrayList<>();
        for (DataForInference dataForInference : dataForInferences) {
            if (dataForInference.getIngestDocumentWrapper().getException() != null
                || CollectionUtils.isEmpty(dataForInference.getInferenceList())) {
                continue;
            }
            inferenceTexts.addAll(dataForInference.getInferenceList());
        }
        return inferenceTexts;
    }

    protected List<DataForInference> getDataForInference(List<IngestDocumentWrapper> ingestDocumentWrappers) {
        List<DataForInference> dataForInferences = new ArrayList<>();
        for (IngestDocumentWrapper ingestDocumentWrapper : ingestDocumentWrappers) {
            Map<String, Object> processMap = null;
            List<String> inferenceList = null;
            IngestDocument ingestDocument = ingestDocumentWrapper.getIngestDocument();
            try {
                preprocessIngestDocument(ingestDocument);
                validateEmbeddingFieldsValue(ingestDocument);
                processMap = buildMapWithTargetKeys(ingestDocument);
                inferenceList = createInferenceList(processMap);
            } catch (Exception e) {
                ingestDocumentWrapper.update(ingestDocument, e);
            } finally {
                dataForInferences.add(new DataForInference(ingestDocumentWrapper, processMap, inferenceList));
            }
        }
        return dataForInferences;
    }

    @Getter
    @AllArgsConstructor
    protected static class DataForInference {
        private final IngestDocumentWrapper ingestDocumentWrapper;
        private final Map<String, Object> processMap;
        private final List<String> inferenceList;
    }

    @SuppressWarnings({ "unchecked" })
    protected List<String> createInferenceList(Map<String, Object> knnKeyMap) {
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
            ((List<String>) sourceValue).stream().filter(Objects::nonNull).forEach(texts::add);
        } else {
            if (sourceValue == null) return;
            texts.add(sourceValue.toString());
        }
    }

    @VisibleForTesting
    Map<String, Object> buildMapWithTargetKeys(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, Object> mapWithProcessorKeys = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            Pair<String, Object> processedNestedKey = processNestedKey(fieldMapEntry);
            String originalKey = processedNestedKey.getKey();
            Object targetKey = processedNestedKey.getValue();

            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildNestedMap(originalKey, targetKey, sourceAndMetadataMap, treeRes);
                mapWithProcessorKeys.put(originalKey, treeRes.get(originalKey));
            } else {
                mapWithProcessorKeys.put(String.valueOf(targetKey), sourceAndMetadataMap.get(originalKey));
            }
        }
        return mapWithProcessorKeys;
    }

    @VisibleForTesting
    void buildNestedMap(String parentKey, Object processorKey, Map<String, Object> sourceAndMetadataMap, Map<String, Object> treeRes) {
        if (Objects.isNull(processorKey) || Objects.isNull(sourceAndMetadataMap)) {
            return;
        }
        if (processorKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            if (sourceAndMetadataMap.get(parentKey) instanceof Map) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    Pair<String, Object> processedNestedKey = processNestedKey(nestedFieldMapEntry);
                    buildNestedMap(
                        processedNestedKey.getKey(),
                        processedNestedKey.getValue(),
                        (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                        next
                    );
                }
            } else if (sourceAndMetadataMap.get(parentKey) instanceof List) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) sourceAndMetadataMap.get(parentKey);
                    Pair<String, Object> processedNestedKey = processNestedKey(nestedFieldMapEntry);
                    List<Object> listOfStrings = list.stream().map(x -> {
                        Object nestedSourceValue = x.get(processedNestedKey.getKey());
                        return normalizeSourceValue(nestedSourceValue);
                    }).collect(Collectors.toList());
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put(processedNestedKey.getKey(), listOfStrings);
                    buildNestedMap(processedNestedKey.getKey(), processedNestedKey.getValue(), map, next);
                }
            }
            treeRes.merge(parentKey, next, REMAPPING_FUNCTION);
        } else {
            String key = String.valueOf(processorKey);
            treeRes.put(key, sourceAndMetadataMap.get(parentKey));
        }
    }

    private boolean isBlankString(Object object) {
        return object instanceof String && StringUtils.isBlank((String) object);
    }

    private Object normalizeSourceValue(Object value) {
        if (isBlankString(value)) {
            return null;
        }
        return value;
    }

    /**
     * Process the nested key, such as "a.b.c" to "a", "b.c"
     * @param nestedFieldMapEntry
     * @return A pair of the original key and the target key
     */
    @VisibleForTesting
    protected Pair<String, Object> processNestedKey(final Map.Entry<String, Object> nestedFieldMapEntry) {
        String originalKey = nestedFieldMapEntry.getKey();
        Object targetKey = nestedFieldMapEntry.getValue();
        int nestedDotIndex = originalKey.indexOf('.');
        if (nestedDotIndex != -1) {
            Map<String, Object> newTargetKey = new LinkedHashMap<>();
            newTargetKey.put(originalKey.substring(nestedDotIndex + 1), targetKey);
            targetKey = newTargetKey;

            originalKey = originalKey.substring(0, nestedDotIndex);
        }
        return new ImmutablePair<>(originalKey, targetKey);
    }

    private void validateEmbeddingFieldsValue(IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        ProcessorDocumentUtils.validateMapTypeValue(
            FIELD_MAP_FIELD,
            sourceAndMetadataMap,
            ProcessorDocumentUtils.unflattenJson(fieldMap),
            indexName,
            clusterService,
            environment,
            false
        );
    }

    protected void setVectorFieldsToDocument(IngestDocument ingestDocument, Map<String, Object> processorMap, List<?> results) {
        Objects.requireNonNull(results, "embedding failed, inference returns null result!");
        log.debug("Model inference result fetched, starting build vector output!");
        Map<String, Object> nlpResult = buildNLPResult(processorMap, results, ingestDocument.getSourceAndMetadata());
        nlpResult.forEach(ingestDocument::setFieldValue);
    }

    /**
     * This method creates a MultiGetRequest from a list of ingest documents to be fetched for comparison
     * @param dataForInferences, list of data for inferences
     * */
    protected MultiGetRequest buildMultiGetRequest(List<DataForInference> dataForInferences) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (DataForInference dataForInference : dataForInferences) {
            Object index = dataForInference.getIngestDocumentWrapper().getIngestDocument().getSourceAndMetadata().get(INDEX_FIELD);
            Object id = dataForInference.getIngestDocumentWrapper().getIngestDocument().getSourceAndMetadata().get(ID_FIELD);
            if (Objects.nonNull(index) && Objects.nonNull(id)) {
                multiGetRequest.add(index.toString(), id.toString());
            }
        }
        return multiGetRequest;
    }

    /**
     * This method creates a map of documents from MultiGetItemResponse where the key is document ID and value is corresponding document
     * @param multiGetItemResponses, array of responses from Multi Get Request
     * */
    protected Map<String, Map<String, Object>> createDocumentMap(MultiGetItemResponse[] multiGetItemResponses) {
        Map<String, Map<String, Object>> existingDocuments = new HashMap<>();
        for (MultiGetItemResponse item : multiGetItemResponses) {
            String id = item.getId();
            Map<String, Object> existingDocument = item.getResponse().getSourceAsMap();
            existingDocuments.put(id, existingDocument);
        }
        return existingDocuments;
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildNLPResult(Map<String, Object> processorMap, List<?> results, Map<String, Object> sourceAndMetadataMap) {
        IndexWrapper indexWrapper = new IndexWrapper(0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> knnMapEntry : processorMap.entrySet()) {
            Pair<String, Object> processedNestedKey = processNestedKey(knnMapEntry);
            String knnKey = processedNestedKey.getKey();
            Object sourceValue = processedNestedKey.getValue();
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
        Map<String, Object> sourceAndMetadataMap
    ) {
        if (processorKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                if (sourceAndMetadataMap.get(processorKey) instanceof List) {
                    if (inputNestedMapEntry.getValue() instanceof List) {
                        processMapEntryValue(
                            results,
                            indexWrapper,
                            (List<Map<String, Object>>) sourceAndMetadataMap.get(processorKey),
                            inputNestedMapEntry.getKey(),
                            (List<Object>) inputNestedMapEntry.getValue()
                        );
                    } else if (inputNestedMapEntry.getValue() instanceof Map) {
                        processMapEntryValue(
                            results,
                            indexWrapper,
                            (List<Map<String, Object>>) sourceAndMetadataMap.get(processorKey),
                            inputNestedMapEntry.getKey(),
                            inputNestedMapEntry.getValue()
                        );
                    }
                } else {
                    Pair<String, Object> processedNestedKey = processNestedKey(inputNestedMapEntry);
                    Map<String, Object> sourceMap = getSourceMapBySourceAndMetadataMap(processorKey, sourceAndMetadataMap);
                    putNLPResultToSourceMapForMapType(
                        processedNestedKey.getKey(),
                        processedNestedKey.getValue(),
                        results,
                        indexWrapper,
                        sourceMap
                    );
                }
            }
        } else if (sourceValue instanceof String) {
            sourceAndMetadataMap.merge(processorKey, results.get(indexWrapper.index++), REMAPPING_FUNCTION);
        } else if (sourceValue instanceof List) {
            sourceAndMetadataMap.merge(
                processorKey,
                buildNLPResultForListType((List<String>) sourceValue, results, indexWrapper),
                REMAPPING_FUNCTION
            );
        }
    }

    private void processMapEntryValue(
        List<?> results,
        IndexWrapper indexWrapper,
        List<Map<String, Object>> sourceAndMetadataMapValueInList,
        String inputNestedMapEntryKey,
        List<Object> inputNestedMapEntryValue
    ) {
        // build nlp output for object in sourceValue which is list type
        Iterator<Object> inputNestedMapValueIt = inputNestedMapEntryValue.iterator();
        for (Map<String, Object> nestedElement : sourceAndMetadataMapValueInList) {
            // Only fill in when value is not null
            if (inputNestedMapValueIt.hasNext() && inputNestedMapValueIt.next() != null) {
                nestedElement.put(inputNestedMapEntryKey, results.get(indexWrapper.index++));
            }
        }
    }

    // This method updates each ingestDocument with exceptions and accepts ingestDocumentWrappers.
    // Ingestion fails when exception occurs while updating
    protected void updateWithExceptions(
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        Consumer<List<IngestDocumentWrapper>> handler,
        Exception e
    ) {
        try {
            for (IngestDocumentWrapper ingestDocumentWrapper : ingestDocumentWrappers) {
                // The IngestDocumentWrapper might have already run into exception. So here we only
                // set exception to IngestDocumentWrapper which doesn't have exception before.
                if (ingestDocumentWrapper.getException() == null) {
                    ingestDocumentWrapper.update(ingestDocumentWrapper.getIngestDocument(), e);
                }
            }
            handler.accept(ingestDocumentWrappers);
        } catch (Exception ex) {
            log.error(String.format(Locale.ROOT, "updating ingestDocumentWrappers with exceptions failed with error: [%s]", ex));
            handler.accept(null);
        }
    }

    private void processMapEntryValue(
        List<?> results,
        IndexWrapper indexWrapper,
        List<Map<String, Object>> sourceAndMetadataMapValueInList,
        String inputNestedMapEntryKey,
        Object inputNestedMapEntryValue
    ) {
        // build nlp output for object in sourceValue which is map type
        Iterator<Map<String, Object>> iterator = sourceAndMetadataMapValueInList.iterator();
        IntStream.range(0, sourceAndMetadataMapValueInList.size()).forEach(index -> {
            Map<String, Object> nestedElement = iterator.next();
            putNLPResultToSingleSourceMapInList(
                inputNestedMapEntryKey,
                inputNestedMapEntryValue,
                results,
                indexWrapper,
                nestedElement,
                index
            );
        });
    }

    /**
     * Put nlp result to single source element, which is in a list field of source document
     * Such source element is in map type
     *
     * @param processorKey
     * @param sourceValue
     * @param results
     * @param indexWrapper
     * @param sourceAndMetadataMap
     * @param nestedElementIndex index of the element in the list field of source document
     */
    @SuppressWarnings("unchecked")
    private void putNLPResultToSingleSourceMapInList(
        String processorKey,
        Object sourceValue,
        List<?> results,
        IndexWrapper indexWrapper,
        Map<String, Object> sourceAndMetadataMap,
        int nestedElementIndex
    ) {
        if (processorKey == null || sourceAndMetadataMap == null || sourceValue == null) return;
        if (sourceValue instanceof Map) {
            for (Map.Entry<String, Object> inputNestedMapEntry : ((Map<String, Object>) sourceValue).entrySet()) {
                Pair<String, Object> processedNestedKey = processNestedKey(inputNestedMapEntry);
                Map<String, Object> sourceMap = getSourceMapBySourceAndMetadataMap(processorKey, sourceAndMetadataMap);
                putNLPResultToSingleSourceMapInList(
                    processedNestedKey.getKey(),
                    processedNestedKey.getValue(),
                    results,
                    indexWrapper,
                    sourceMap,
                    nestedElementIndex
                );
            }
        } else {
            if (sourceValue instanceof List && ((List<Object>) sourceValue).get(nestedElementIndex) != null) {
                sourceAndMetadataMap.merge(processorKey, results.get(indexWrapper.index++), REMAPPING_FUNCTION);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSourceMapBySourceAndMetadataMap(String processorKey, Map<String, Object> sourceAndMetadataMap) {
        Map<String, Object> sourceMap = new HashMap<>();
        if (sourceAndMetadataMap.get(processorKey) == null) {
            sourceAndMetadataMap.put(processorKey, sourceMap);
        } else {
            sourceMap = (Map<String, Object>) sourceAndMetadataMap.get(processorKey);
        }
        return sourceMap;
    }

    private List<Map<String, Object>> buildNLPResultForListType(List<String> sourceValue, List<?> results, IndexWrapper indexWrapper) {
        List<Map<String, Object>> keyToResult = new ArrayList<>();
        sourceValue.stream()
            .filter(Objects::nonNull) // explicit null check is required since sourceValue can contain null values in cases where
            // sourceValue has been filtered
            .forEachOrdered(x -> keyToResult.add(ImmutableMap.of(listTypeNestedMapKey, results.get(indexWrapper.index++))));
        return keyToResult;
    }

    // This method validates and filters given inferenceList and processMap after response is successfully retrieved from get operation.
    protected void reuseOrGenerateEmbedding(
        GetResponse response,
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler,
        InferenceFilter inferenceFilter
    ) {
        final Map<String, Object> existingDocument = response.getSourceAsMap();
        if (existingDocument == null || existingDocument.isEmpty()) {
            generateAndSetInference(ingestDocument, processMap, inferenceList, handler);
            return;
        }
        // filter given ProcessMap by comparing existing document with ingestDocument
        Map<String, Object> filteredProcessMap = inferenceFilter.filterAndCopyExistingEmbeddings(
            existingDocument,
            ingestDocument.getSourceAndMetadata(),
            processMap
        );
        // create inference list based on filtered ProcessMap
        List<String> filteredInferenceList = createInferenceList(filteredProcessMap).stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        if (filteredInferenceList.isEmpty()) {
            handler.accept(ingestDocument, null);
        } else {
            generateAndSetInference(ingestDocument, filteredProcessMap, filteredInferenceList, handler);
        }
    }

    // This method validates and filters given inferenceList and dataForInferences after response is successfully retrieved from multi-get
    // operation.
    protected void reuseOrGenerateEmbedding(
        MultiGetResponse response,
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        List<String> inferenceList,
        List<DataForInference> dataForInferences,
        Consumer<List<IngestDocumentWrapper>> handler,
        InferenceFilter inferenceFilter
    ) {
        MultiGetItemResponse[] multiGetItemResponses = response.getResponses();
        if (multiGetItemResponses == null || multiGetItemResponses.length == 0) {
            doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, handler);
            return;
        }
        // create a map of documents with key: doc_id and value: doc
        Map<String, Map<String, Object>> existingDocuments = createDocumentMap(multiGetItemResponses);
        List<DataForInference> filteredDataForInference = filterDataForInference(inferenceFilter, dataForInferences, existingDocuments);
        List<String> filteredInferenceList = constructInferenceTexts(filteredDataForInference);
        if (filteredInferenceList.isEmpty()) {
            handler.accept(ingestDocumentWrappers);
            return;
        }
        doSubBatchExecute(ingestDocumentWrappers, filteredInferenceList, filteredDataForInference, handler);
    }

    // This is a helper method to filter the given list of dataForInferences by comparing its documents with existingDocuments with
    // given inferenceFilter
    protected List<DataForInference> filterDataForInference(
        InferenceFilter inferenceFilter,
        List<DataForInference> dataForInferences,
        Map<String, Map<String, Object>> existingDocuments
    ) {
        List<DataForInference> filteredDataForInference = new ArrayList<>();
        for (DataForInference dataForInference : dataForInferences) {
            IngestDocumentWrapper ingestDocumentWrapper = dataForInference.getIngestDocumentWrapper();
            Map<String, Object> processMap = dataForInference.getProcessMap();
            Map<String, Object> document = ingestDocumentWrapper.getIngestDocument().getSourceAndMetadata();
            Object id = document.get(ID_FIELD);
            // insert non-filtered dataForInference if existing document does not exist
            if (Objects.isNull(id) || existingDocuments.containsKey(id.toString()) == false) {
                filteredDataForInference.add(dataForInference);
                continue;
            }
            // filter dataForInference when existing document exists
            String docId = id.toString();
            Map<String, Object> existingDocument = existingDocuments.get(docId);
            Map<String, Object> filteredProcessMap = inferenceFilter.filterAndCopyExistingEmbeddings(
                existingDocument,
                document,
                processMap
            );
            List<String> filteredInferenceList = createInferenceList(filteredProcessMap);
            filteredDataForInference.add(new DataForInference(ingestDocumentWrapper, filteredProcessMap, filteredInferenceList));
        }
        return filteredDataForInference;
    }

    // form and return a list of IngestDocumentWrapper in given list of DataForInference
    protected List<IngestDocumentWrapper> getIngestDocumentWrappers(List<DataForInference> dataForInferences) {
        return dataForInferences.stream().map(DataForInference::getIngestDocumentWrapper).toList();
    }

    /**
     * This method invokes inference call through mlCommonsClientAccessor and populates retrieved embeddings to ingestDocument
     *
     * @param ingestDocument ingestDocument to populate embeddings to
     * @param processMap map indicating the path in ingestDocument to populate embeddings
     * @param inferenceList list of texts to be model inference
     * @param handler SourceAndMetadataMap of ingestDocument Document
     *
     */
    protected void generateAndSetInference(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentences(
            TextInferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).build(),
            ActionListener.wrap(vectors -> {
                setVectorFieldsToDocument(ingestDocument, processMap, vectors);
                handler.accept(ingestDocument, null);
            }, e -> { handler.accept(null, e); })
        );
    }

    /**
     * This method invokes inference call that returns results as map through mlCommonsClientAccessor and populates retrieved embeddings to ingestDocument
     *
     * @param ingestDocument ingestDocument to populate embeddings to
     * @param processMap map indicating the path in ingestDocument to populate embeddings
     * @param inferenceList list of texts to be model inference
     * @param pruneType    The type of prune strategy to use
     * @param pruneRatio   The ratio or threshold for prune
     * @param handler handler for accepting IngestionDocument
     *
     */
    protected void generateAndSetMapInference(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        PruneType pruneType,
        float pruneRatio,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).build(),
            ActionListener.wrap(resultMaps -> {
                List<Map<String, Float>> sparseVectors = TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)
                    .stream()
                    .map(vector -> PruneUtils.pruneSparseVector(pruneType, pruneRatio, vector))
                    .toList();
                setVectorFieldsToDocument(ingestDocument, processMap, sparseVectors);
                handler.accept(ingestDocument, null);
            }, e -> { handler.accept(null, e); })
        );
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
        private int index;

        protected IndexWrapper(int index) {
            this.index = index;
        }
    }
}
