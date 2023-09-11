/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * This processor is used for user input data text embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the text embedding results.
 */
@Log4j2
public class TextEmbeddingProcessor extends NLPProcessor {

    public static final String TYPE = "text_embedding";

    public TextEmbeddingProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description, modelId, fieldMap, clientAccessor, environment);
        this.LIST_TYPE_NESTED_MAP_KEY = "knn";
    }

    @Override
    public void doExecute(IngestDocument ingestDocument, Map<String, Object> ProcessMap, List<String> inferenceList, BiConsumer<IngestDocument, Exception> handler) {
        mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(vectors -> {
            setVectorFieldsToDocument(ingestDocument, ProcessMap, vectors);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
