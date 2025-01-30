/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.DEFAULT_IGNORE_UNALTERED;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.IGNORE_UNALTERED;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;

import java.util.Map;

import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;

/**
 * Factory for text embedding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
public final class TextEmbeddingProcessorFactory extends AbstractBatchingProcessor.Factory {

    private final OpenSearchClient openSearchClient;

    private final MLCommonsClientAccessor clientAccessor;

    private final Environment environment;

    private final ClusterService clusterService;

    public TextEmbeddingProcessorFactory(
        final OpenSearchClient openSearchClient,
        final MLCommonsClientAccessor clientAccessor,
        final Environment environment,
        final ClusterService clusterService
    ) {
        super(TYPE);
        this.openSearchClient = openSearchClient;
        this.clientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
    }

    @Override
    protected AbstractBatchingProcessor newProcessor(String tag, String description, int batchSize, Map<String, Object> config) {
        String modelId = readStringProperty(TYPE, tag, config, MODEL_ID_FIELD);
        Map<String, Object> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
        boolean ignoreUnaltered = readBooleanProperty(TYPE, tag, config, IGNORE_UNALTERED, DEFAULT_IGNORE_UNALTERED);
        return new TextEmbeddingProcessor(
            tag,
            description,
            batchSize,
            modelId,
            fieldMap,
            ignoreUnaltered,
            openSearchClient,
            clientAccessor,
            environment,
            clusterService
        );
    }
}
