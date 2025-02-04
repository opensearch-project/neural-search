/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import lombok.extern.log4j.Log4j2;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 */
@Log4j2
public final class TextEmbeddingProcessor extends InferenceProcessor {

    public static final String TYPE = "text_embedding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "knn";
    public static final String IGNORE_UNALTERED = "ignore_unaltered";
    public static final boolean DEFAULT_IGNORE_UNALTERED = false;
    private final boolean ignoreUnaltered;
    private final OpenSearchClient openSearchClient;

    public TextEmbeddingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        boolean ignoreUnaltered,
        OpenSearchClient openSearchClient,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
        this.openSearchClient = openSearchClient;
        this.ignoreUnaltered = ignoreUnaltered;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        if (ignoreUnaltered == true) {
            String index = ingestDocument.getSourceAndMetadata().get("_index").toString();
            String id = ingestDocument.getSourceAndMetadata().get("_id").toString();
            openSearchClient.execute(GetAction.INSTANCE, new GetRequest(index, id), ActionListener.wrap(response -> {
                final Map<String, Object> document = response.getSourceAsMap();
                if (document == null || document.isEmpty()) {
                    makeInferenceCall(ingestDocument, ProcessMap, inferenceList, handler);
                } else {
                    Map<String, Object> filteredProcessMap = filterProcessMap(document, ingestDocument.getSourceAndMetadata(), ProcessMap);
                    List<String> filteredInferenceList = createInferenceList(filteredProcessMap);
                    if (!filteredInferenceList.isEmpty()) {
                        log.info("making inference call for: {}", filteredInferenceList);
                        makeInferenceCall(ingestDocument, filteredProcessMap, filteredInferenceList, handler);
                    } else {
                        log.info("skipping inference call");
                        handler.accept(ingestDocument, null);
                    }
                }
            }, e -> { makeInferenceCall(ingestDocument, ProcessMap, inferenceList, handler); }));
        } else {
            makeInferenceCall(ingestDocument, ProcessMap, inferenceList, handler);
        }
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(handler::accept, onException));
    }
}
