/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.optimization.TextEmbeddingInferenceFilter;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 * If skip_existing flag is on, Get/MultiGet request is made to compare between new document and existing document to skip existing embeddings
 */
@Log4j2
public final class TextEmbeddingProcessor extends InferenceProcessor {

    public static final String TYPE = "text_embedding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "knn";
    public static final String SKIP_EXISTING = "skip_existing";
    public static final boolean DEFAULT_SKIP_EXISTING = false;
    private final OpenSearchClient openSearchClient;
    private final boolean skipExisting;
    private final TextEmbeddingInferenceFilter textEmbeddingInferenceFilter;

    public TextEmbeddingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        boolean skipExisting,
        TextEmbeddingInferenceFilter textEmbeddingInferenceFilter,
        OpenSearchClient openSearchClient,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
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
        // skip existing flag is turned off. Call model inference without filtering
        if (skipExisting == false) {
            makeInferenceCall(ingestDocument, processMap, inferenceList, handler);
            return;
        }
        // if skipExisting flag is turned on, eligible inference texts will be compared and filtered after embeddings are copied
        String index = ingestDocument.getSourceAndMetadata().get(INDEX_FIELD).toString();
        String id = ingestDocument.getSourceAndMetadata().get(ID_FIELD).toString();
        openSearchClient.execute(GetAction.INSTANCE, new GetRequest(index, id), ActionListener.wrap(response -> {
            final Map<String, Object> existingDocument = response.getSourceAsMap();
            if (existingDocument == null || existingDocument.isEmpty()) {
                makeInferenceCall(ingestDocument, processMap, inferenceList, handler);
                return;
            }
            // filter given ProcessMap by comparing existing document with ingestDocument
            Map<String, Object> filteredProcessMap = textEmbeddingInferenceFilter.filter(
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
                makeInferenceCall(ingestDocument, filteredProcessMap, filteredInferenceList, handler);
            }

        }, e -> { handler.accept(null, e); }));
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentences(
            TextInferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).build(),
            ActionListener.wrap(handler::accept, onException)
        );
    }

    @Override
    public void batchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        // skip existing flag is turned off. Call existing batchExecute without filtering
        if (skipExisting == false) {
            super.batchExecute(ingestDocumentWrappers, handler);
            return;
        }
        if (ingestDocumentWrappers.isEmpty()) {
            handler.accept(Collections.emptyList());
            return;
        }
        // if skipExisting flag is turned on, inference texts in each document will be compared with existing documents fetched via
        // MultiGet. TextEmbeddingInferenceFilter will be used to filter each inference texts
        openSearchClient.execute(MultiGetAction.INSTANCE, buildMultiGetRequest(ingestDocumentWrappers), ActionListener.wrap(response -> {
            MultiGetItemResponse[] multiGetItemResponses = response.getResponses();
            if (multiGetItemResponses == null || multiGetItemResponses.length == 0) {
                super.batchExecute(ingestDocumentWrappers, handler);
                return;
            }
            Map<String, Map<String, Object>> existingDocuments = createDocumentMap(multiGetItemResponses);
            if (this.batchSize >= ingestDocumentWrappers.size()) {
                subBatchExecuteWithFilter(ingestDocumentWrappers, existingDocuments, textEmbeddingInferenceFilter, handler);
                return;
            }
            List<List<IngestDocumentWrapper>> batches = cutBatches(ingestDocumentWrappers);
            int size = ingestDocumentWrappers.size();
            AtomicInteger counter = new AtomicInteger(size);
            List<IngestDocumentWrapper> allResults = Collections.synchronizedList(new ArrayList());
            for (List<IngestDocumentWrapper> batch : batches) {
                this.subBatchExecuteWithFilter(batch, existingDocuments, textEmbeddingInferenceFilter, (batchResults) -> {
                    allResults.addAll(batchResults);
                    if (counter.addAndGet(-batchResults.size()) == 0) {
                        handler.accept(allResults);
                    }
                    assert counter.get() >= 0 : "counter is negative";
                });
            }
        }, e -> { handler.accept(null); }));
    }
}
