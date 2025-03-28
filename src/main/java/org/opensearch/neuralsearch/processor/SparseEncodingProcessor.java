/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.Getter;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.optimization.TextEmbeddingInferenceFilter;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.transport.client.OpenSearchClient;

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

    @Getter
    private final PruneType pruneType;
    @Getter
    private final float pruneRatio;

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
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        if (skipExisting == false) {
            generateAndSetMapInference(ingestDocument, processMap, inferenceList, pruneType, pruneRatio, handler);
            return;
        }
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
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).build(),
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
}
