/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import lombok.extern.log4j.Log4j2;

import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 */
@Log4j2
public class TextEmbeddingProcessor extends NLPProcessor {

    public static final String TYPE = "text_embedding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "knn";

    public TextEmbeddingProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment);
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(vectors -> {
            setVectorFieldsToDocument(ingestDocument, ProcessMap, vectors);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }
}
