/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;

/**
 * Factory for text embedding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
public class TextEmbeddingProcessorFactory implements Processor.Factory {

    private final MLCommonsClientAccessor clientAccessor;

    private final Environment environment;

    private final ClusterService clusterService;

    public TextEmbeddingProcessorFactory(
        final MLCommonsClientAccessor clientAccessor,
        final Environment environment,
        final ClusterService clusterService
    ) {
        this.clientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
    }

    @Override
    public TextEmbeddingProcessor create(
        final Map<String, Processor.Factory> registry,
        final String processorTag,
        final String description,
        final Map<String, Object> config
    ) throws Exception {
        String modelId = readStringProperty(TYPE, processorTag, config, MODEL_ID_FIELD);
        Map<String, Object> filedMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
        return new TextEmbeddingProcessor(processorTag, description, modelId, filedMap, clientAccessor, environment, clusterService);
    }
}
