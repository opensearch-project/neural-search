/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.input.parameter.textembedding.SparseEmbeddingFormat;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.optimization.TextEmbeddingInferenceFilter;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.transport.client.OpenSearchClient;

import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.EmbeddingContentType.PASSAGE;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.DocFieldNames.ID_FIELD;
import static org.opensearch.neuralsearch.constants.DocFieldNames.INDEX_FIELD;

/**
 * This processor is used for user input data text sparse encoding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the sparse encoding results.
 */
@Log4j2
public final class SparseEncodingProcessor extends InferenceProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "sparse_encoding";
    private final OpenSearchClient openSearchClient;
    private final boolean skipExisting;
    private final TextEmbeddingInferenceFilter textEmbeddingInferenceFilter;
    private static final AsymmetricTextEmbeddingParameters TOKEN_ID_PARAMETER = AsymmetricTextEmbeddingParameters.builder()
        .sparseEmbeddingFormat(SparseEmbeddingFormat.TOKEN_ID)
        .build();

    @Getter
    private final PruneType pruneType;
    @Getter
    private final float pruneRatio;
    private final ClusterService clusterService;
    private final Environment environment;

    public SparseEncodingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        boolean skipExisting,
        TextEmbeddingInferenceFilter textEmbeddingInferenceFilter,
        PruneType pruneType,
        float pruneRatio,
        OpenSearchClient openSearchClient,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
        this.pruneType = pruneType;
        this.pruneRatio = pruneRatio;
        this.skipExisting = skipExisting;
        this.textEmbeddingInferenceFilter = textEmbeddingInferenceFilter;
        this.openSearchClient = openSearchClient;
        this.clusterService = clusterService;
        this.environment = environment;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        EventStatsManager.increment(EventStatName.SPARSE_ENCODING_PROCESSOR_EXECUTIONS);
        if (skipExisting == false) {
            generateAndSetMapInference(ingestDocument, processMap, inferenceList, pruneType, pruneRatio, handler);
            return;
        }
        EventStatsManager.increment(EventStatName.SKIP_EXISTING_EXECUTIONS);
        // if skipExisting flag is turned on, eligible inference texts will be compared and filtered after embeddings are copied
        Object index = ingestDocument.getSourceAndMetadata().get(INDEX_FIELD);
        Object id = ingestDocument.getSourceAndMetadata().get(ID_FIELD);
        if (Objects.isNull(index) || Objects.isNull(id)) {
            generateAndSetMapInference(ingestDocument, processMap, inferenceList, pruneType, pruneRatio, handler);
            return;
        }
        openSearchClient.execute(GetAction.INSTANCE, new GetRequest(index.toString(), id.toString()), ActionListener.wrap(response -> {
            final Map<String, Object> existingDocument = response.getSourceAsMap();
            if (existingDocument == null || existingDocument.isEmpty()) {
                generateAndSetMapInference(ingestDocument, processMap, inferenceList, pruneType, pruneRatio, handler);
                return;
            }
            // filter given ProcessMap by comparing existing document with ingestDocument
            Map<String, Object> filteredProcessMap = textEmbeddingInferenceFilter.filterAndCopyExistingEmbeddings(
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
                generateAndSetMapInference(ingestDocument, filteredProcessMap, filteredInferenceList, pruneType, pruneRatio, handler);
            }

        }, e -> { handler.accept(null, e); }));
    }

    @Override
    protected void doSubBatchExecute(
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        List<String> inferenceList,
        List<DataForInference> dataForInferences,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        if (CollectionUtils.isEmpty(ingestDocumentWrappers)) {
            handler.accept(ingestDocumentWrappers);
            return;
        }
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/ingest/IngestService.java#L921-L935
        // ingest documents in a batch belong to the same index
        Object indexObj = ingestDocumentWrappers.getFirst().getIngestDocument().getSourceAndMetadata().get(INDEX_FIELD);
        String index = indexObj.toString();
        long maxDepth = SparseFieldUtils.getMaxDepth(index, clusterService);
        Set<String> sparseAnnFields = SparseFieldUtils.getSparseAnnFields(index, clusterService, maxDepth);
        if (sparseAnnFields.isEmpty()) {
            super.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, handler);
            return;
        }
        SplitDataResponse splitDataResponse = splitData(dataForInferences, sparseAnnFields, maxDepth);
        AtomicInteger counter = new AtomicInteger(0);
        if (splitDataResponse.getTokenIdDataForInference().isEmpty()) {
            super.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, handler);
            return;
        } else {
            counter.incrementAndGet();
        }
        if (!splitDataResponse.getWordDataForInference().isEmpty()) {
            counter.incrementAndGet();
        }
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        if (!splitDataResponse.getTokenIdDataForInference().isEmpty()) {
            doBatchExecuteWithType(
                splitDataResponse.getTokenIdResponseInferenceList(),
                splitDataResponse.getTokenIdDataForInference(),
                SparseEmbeddingFormat.TOKEN_ID,
                getCountDownBatchDataHandler(counter, ingestDocumentWrappers, exceptions, handler)
            );
        }
        if (!splitDataResponse.wordDataForInference.isEmpty()) {
            doBatchExecuteWithType(
                splitDataResponse.getWordResponseInferenceList(),
                splitDataResponse.getWordDataForInference(),
                SparseEmbeddingFormat.WORD,
                getCountDownBatchDataHandler(counter, ingestDocumentWrappers, exceptions, handler)
            );
        }
    }

    private void doBatchExecuteWithType(
        final List<String> inferenceList,
        final List<DataForInference> dataForInferences,
        SparseEmbeddingFormat format,
        BiConsumer<List<DataForInference>, Exception> handler
    ) {
        Tuple<List<String>, Map<Integer, Integer>> sortedResult = sortByLengthAndReturnOriginalOrder(inferenceList);
        List<String> sortedInferenceList = sortedResult.v1();
        Map<Integer, Integer> originalOrder = sortedResult.v2();
        final AsymmetricTextEmbeddingParameters parameters = format == SparseEmbeddingFormat.TOKEN_ID ? TOKEN_ID_PARAMETER : null;
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder()
                .modelId(this.modelId)
                .inputTexts(sortedInferenceList)
                .mlAlgoParams(parameters)
                .embeddingContentType(PASSAGE)
                .build(),
            ActionListener.wrap(resultMaps -> {
                List<Map<String, Float>> sparseVectors = TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)
                    .stream()
                    .map(vector -> PruneUtils.pruneSparseVector(pruneType, pruneRatio, vector))
                    .toList();
                batchExecuteHandler(sparseVectors, dataForInferences, originalOrder);
                handler.accept(dataForInferences, null);
            }, exception -> { handler.accept(dataForInferences, exception); })
        );
    }

    private SplitDataResponse splitData(List<DataForInference> dataForInferences, Set<String> sparseAnnFields, long maxDepth) {
        SplitDataResponse splitDataResponse = new SplitDataResponse();
        for (DataForInference dataForInference : dataForInferences) {
            Map<String, Object> tokenIdProcessMap = new HashMap<>();
            Map<String, Object> wordProcessMap = new HashMap<>();
            splitProcessMap(dataForInference.getProcessMap(), sparseAnnFields, "", tokenIdProcessMap, wordProcessMap, 1, maxDepth);
            List<String> tokenIdList = createInferenceList(tokenIdProcessMap);
            List<String> wordList = createInferenceList(wordProcessMap);
            splitDataResponse.getTokenIdResponseInferenceList().addAll(tokenIdList);
            splitDataResponse.getWordResponseInferenceList().addAll(wordList);
            if (!tokenIdList.isEmpty()) {
                splitDataResponse.getTokenIdDataForInference()
                    .add(new DataForInference(dataForInference.getIngestDocumentWrapper(), tokenIdProcessMap, tokenIdList));
            }
            if (!wordList.isEmpty()) {
                splitDataResponse.getWordDataForInference()
                    .add(new DataForInference(dataForInference.getIngestDocumentWrapper(), wordProcessMap, wordList));
            }
        }
        return splitDataResponse;
    }

    /**
     * Recursively splits the processMap into tokenId and word maps based on sparse ANN field detection.
     *
     * @param processMap The current level of the process map
     * @param sparseAnnFields Set of sparse ANN field paths
     * @param currentPath The current path being processed
     * @param tokenIdProcessMap Map to collect token ID fields
     * @param wordProcessMap Map to collect word fields
     * @param depth Current recursion depth
     * @param maxDepth Maximum allowed recursion depth
     */
    @SuppressWarnings("unchecked")
    private void splitProcessMap(
        Map<String, Object> processMap,
        Set<String> sparseAnnFields,
        String currentPath,
        Map<String, Object> tokenIdProcessMap,
        Map<String, Object> wordProcessMap,
        int depth,
        long maxDepth
    ) {
        validateDepth(currentPath, depth, maxDepth);

        for (Map.Entry<String, Object> entry : processMap.entrySet()) {
            String fieldPath = currentPath.isEmpty() ? entry.getKey() : currentPath + "." + entry.getKey();

            if (entry.getValue() instanceof Map) {
                Map<String, Object> nestedTokenIdMap = new HashMap<>();
                Map<String, Object> nestedWordMap = new HashMap<>();
                splitProcessMap(
                    (Map<String, Object>) entry.getValue(),
                    sparseAnnFields,
                    fieldPath,
                    nestedTokenIdMap,
                    nestedWordMap,
                    depth + 1,
                    maxDepth
                );

                if (!nestedTokenIdMap.isEmpty()) {
                    tokenIdProcessMap.put(entry.getKey(), nestedTokenIdMap);
                }
                if (!nestedWordMap.isEmpty()) {
                    wordProcessMap.put(entry.getKey(), nestedWordMap);
                }
            } else if (isSparseAnnFieldByPath(sparseAnnFields, fieldPath)) {
                tokenIdProcessMap.put(entry.getKey(), entry.getValue());
            } else {
                wordProcessMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void validateDepth(String fieldPath, int depth, long maxDepth) {
        if (depth > maxDepth) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Field [%s] exceeds maximum depth limit of [%d], cannot process it", fieldPath, maxDepth)
            );
        }
    }

    /**
     * Checks if a field path matches a sparse ANN field.
     * This method checks both exact matches and prefix matches to handle cases where
     * the processMap structure may not include the listTypeNestedMapKey.
     *
     * For example, if sparseAnnFields contains "parent.passage_chunk_embedding.sparse_encoding"
     * and fieldPath is "parent.passage_chunk_embedding", this returns true because the fieldPath
     * is a prefix of a sparse ANN field.
     *
     * @param sparseAnnFields Set of sparse ANN field paths (full paths to sparse_vector fields)
     * @param fieldPath The full field path to check
     * @return true if the field path matches or is a prefix of any sparse ANN field
     */
    private boolean isSparseAnnFieldByPath(Set<String> sparseAnnFields, String fieldPath) {
        return sparseAnnFields.contains(fieldPath)
            || !fieldPath.isEmpty() && sparseAnnFields.contains(fieldPath + "." + this.listTypeNestedMapKey);
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).embeddingContentType(PASSAGE).build(),
            ActionListener.wrap(resultMaps -> {
                List<Map<String, Float>> sparseVectors = TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)
                    .stream()
                    .map(vector -> PruneUtils.pruneSparseVector(pruneType, pruneRatio, vector))
                    .toList();
                handler.accept(sparseVectors);
            }, onException)
        );
    }

    @Override
    public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        EventStatsManager.increment(EventStatName.SPARSE_ENCODING_PROCESSOR_EXECUTIONS);
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
            // skip existing flag is turned off. Call doSubBatchExecute without filtering
            if (skipExisting == false) {
                doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, handler);
                return;
            }
            // skipExisting flag is turned on, eligible inference texts in dataForInferences will be compared and filtered after embeddings
            // are copied
            openSearchClient.execute(
                MultiGetAction.INSTANCE,
                buildMultiGetRequest(dataForInferences),
                ActionListener.wrap(
                    response -> reuseOrGenerateEmbedding(
                        response,
                        ingestDocumentWrappers,
                        inferenceList,
                        dataForInferences,
                        handler,
                        textEmbeddingInferenceFilter
                    ),
                    e -> {
                        // When exception is thrown in for MultiGetAction, set exception to all ingestDocumentWrappers
                        updateWithExceptions(getIngestDocumentWrappers(dataForInferences), handler, e);
                    }
                )
            );
        } catch (Exception e) {
            updateWithExceptions(ingestDocumentWrappers, handler, e);
        }
    }

    private void generateAndSetMapInference(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        PruneType pruneType,
        float pruneRatio,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        Object indexObj = ingestDocument.getSourceAndMetadata().get(INDEX_FIELD);
        String index = indexObj == null ? null : indexObj.toString();
        long maxDepth = SparseFieldUtils.getMaxDepth(index, clusterService);
        Set<String> sparseAnnFields = SparseFieldUtils.getSparseAnnFields(index, clusterService, maxDepth);
        Map<String, Object> tokenIdProcessMap = new HashMap<>();
        Map<String, Object> wordProcessMap = new HashMap<>();
        splitProcessMap(processMap, sparseAnnFields, "", tokenIdProcessMap, wordProcessMap, 1, maxDepth);
        AtomicInteger counter = new AtomicInteger(0);
        if (tokenIdProcessMap.isEmpty()) {
            generateAndSetMapInference(ingestDocument, processMap, inferenceList, pruneType, pruneRatio, null, handler);
            return;
        } else {
            counter.incrementAndGet();
        }
        if (!wordProcessMap.isEmpty()) {
            counter.incrementAndGet();
        }
        if (counter.get() == 0) {
            handler.accept(ingestDocument, null);
            return;
        }
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        if (!tokenIdProcessMap.isEmpty()) {
            List<String> updatedInferenceList = createInferenceList(tokenIdProcessMap);
            generateAndSetMapInference(
                ingestDocument,
                tokenIdProcessMap,
                updatedInferenceList,
                pruneType,
                pruneRatio,
                TOKEN_ID_PARAMETER,
                getCountDownHandler(counter, exceptions, handler)
            );
        }
        if (!wordProcessMap.isEmpty()) {
            List<String> updatedInferenceList = createInferenceList(wordProcessMap);
            generateAndSetMapInference(
                ingestDocument,
                wordProcessMap,
                updatedInferenceList,
                pruneType,
                pruneRatio,
                null,
                getCountDownHandler(counter, exceptions, handler)
            );
        }
    }

    private BiConsumer<IngestDocument, Exception> getCountDownHandler(
        AtomicInteger counter,
        List<Exception> exceptions,
        BiConsumer<IngestDocument, Exception> originalHandler
    ) {
        return (ingestDocument, e) -> {
            if (e != null) {
                exceptions.add(e);
            }
            if (counter.decrementAndGet() == 0) {
                if (!exceptions.isEmpty()) {
                    originalHandler.accept(null, exceptions.getFirst());
                } else {
                    originalHandler.accept(ingestDocument, null);
                }
            }
        };
    }

    private BiConsumer<List<DataForInference>, Exception> getCountDownBatchDataHandler(
        AtomicInteger counter,
        List<IngestDocumentWrapper> ingestDocumentWrappers,
        List<Exception> exceptions,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        return (dataForInferences, e) -> {
            if (e != null) {
                exceptions.add(e);
            }
            if (counter.decrementAndGet() == 0) {
                if (!exceptions.isEmpty()) {
                    updateWithExceptions(ingestDocumentWrappers, handler, exceptions.getFirst());
                } else {
                    handler.accept(ingestDocumentWrappers);
                }
            }
        };
    }

    @Data
    private static class SplitDataResponse {
        private List<String> tokenIdResponseInferenceList = new ArrayList<>();
        private List<String> wordResponseInferenceList = new ArrayList<>();
        private List<DataForInference> tokenIdDataForInference = new ArrayList<>();
        private List<DataForInference> wordDataForInference = new ArrayList<>();
    }
}
