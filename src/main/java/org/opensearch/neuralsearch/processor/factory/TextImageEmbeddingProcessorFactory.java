/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.DEFAULT_SKIP_EXISTING;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.SKIP_EXISTING;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.EMBEDDING_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.TYPE;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;

import lombok.AllArgsConstructor;
import org.opensearch.neuralsearch.processor.optimization.TextImageEmbeddingInferenceFilter;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Factory for text_image embedding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
@AllArgsConstructor
public class TextImageEmbeddingProcessorFactory implements Processor.Factory {

    private final OpenSearchClient openSearchClient;
    private final MLCommonsClientAccessor clientAccessor;
    private final Environment environment;
    private final ClusterService clusterService;

    @Override
    public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config)
        throws Exception {
        String modelId = readStringProperty(TYPE, tag, config, MODEL_ID_FIELD);
        String embedding = readStringProperty(TYPE, tag, config, EMBEDDING_FIELD);
        Map<String, String> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
        boolean skipExisting = readBooleanProperty(TextImageEmbeddingProcessor.TYPE, tag, config, SKIP_EXISTING, DEFAULT_SKIP_EXISTING);
        return new TextImageEmbeddingProcessor(
            tag,
            description,
            modelId,
            embedding,
            fieldMap,
            skipExisting,
            skipExisting ? new TextImageEmbeddingInferenceFilter() : null,
            openSearchClient,
            clientAccessor,
            environment,
            clusterService
        );
    }
}
