/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.Factory;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.EMBEDDING_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.TYPE;

import java.util.Map;

import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;

/**
 * Factory for text_image embedding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
public class TextImageEmbeddingProcessorFactory implements Factory {

    private final MLCommonsClientAccessor clientAccessor;

    private final Environment environment;

    public TextImageEmbeddingProcessorFactory(final MLCommonsClientAccessor clientAccessor, final Environment environment) {
        this.clientAccessor = clientAccessor;
        this.environment = environment;
    }

    @Override
    public TextImageEmbeddingProcessor create(
        final Map<String, Factory> registry,
        final String processorTag,
        final String description,
        final Map<String, Object> config
    ) throws Exception {
        String modelId = readStringProperty(TYPE, processorTag, config, MODEL_ID_FIELD);
        String embedding = readStringProperty(TYPE, processorTag, config, EMBEDDING_FIELD);
        Map<String, String> filedMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
        return new TextImageEmbeddingProcessor(processorTag, description, modelId, embedding, filedMap, clientAccessor, environment);
    }
}
