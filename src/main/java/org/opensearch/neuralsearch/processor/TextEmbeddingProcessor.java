/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters.EmbeddingContentType;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor.InferenceRequest;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to
 * indicate which model user use, and field_map can be used to indicate which fields needs text
 * embedding and the corresponding keys for the text embedding results.
 */
@Log4j2
public final class TextEmbeddingProcessor extends InferenceProcessor {

    public static final String TYPE = "text_embedding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "knn";

    private static final AsymmetricTextEmbeddingParameters PASSAGE_PARAMETERS = AsymmetricTextEmbeddingParameters.builder()
        .embeddingContentType(EmbeddingContentType.PASSAGE)
        .build();

    public TextEmbeddingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentences(
            InferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).mlAlgoParams(PASSAGE_PARAMETERS).build(),
            ActionListener.wrap(vectors -> {
                setVectorFieldsToDocument(ingestDocument, ProcessMap, vectors);
                handler.accept(ingestDocument, null);
            }, e -> { handler.accept(null, e); })
        );
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentences(
            InferenceRequest.builder().modelId(this.modelId).inputTexts(inferenceList).mlAlgoParams(PASSAGE_PARAMETERS).build(),
            ActionListener.wrap(handler::accept, onException)
        );
    }
}
