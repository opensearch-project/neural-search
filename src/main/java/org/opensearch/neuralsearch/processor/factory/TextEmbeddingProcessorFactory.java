/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.InferenceProcessor.BATCH_SIZE_BYTES_FIELD;
import static org.opensearch.neuralsearch.processor.InferenceProcessor.DEFAULT_BATCH_SIZE_BYTES;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.SKIP_EXISTING;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.DEFAULT_SKIP_EXISTING;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.InferenceProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.optimization.TextEmbeddingInferenceFilter;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Factory for text embedding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
public final class TextEmbeddingProcessorFactory extends AbstractBatchingProcessor.Factory {

    private final OpenSearchClient openSearchClient;

    private final MLCommonsClientAccessor clientAccessor;

    private final Environment environment;

    private final ClusterService clusterService;

    /**
     * Holds whether the user explicitly set batch_size in the pipeline config for the current
     * create() call. This is needed because AbstractBatchingProcessor.Factory.create() removes
     * batch_size from the config map (via ConfigurationUtils.readIntProperty) before calling
     * newProcessor(), making it impossible to detect user intent inside newProcessor() directly.
     * ThreadLocal ensures thread safety since factory instances are shared across concurrent
     * pipeline creation calls.
     */
    private final ThreadLocal<Boolean> userSetBatchSize = new ThreadLocal<>();

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
    public AbstractBatchingProcessor create(
        Map<String, Processor.Factory> processorFactories,
        String tag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        // Peek before super.create() removes batch_size from the config map
        userSetBatchSize.set(config.containsKey(AbstractBatchingProcessor.BATCH_SIZE_FIELD));
        try {
            return super.create(processorFactories, tag, description, config);
        } finally {
            userSetBatchSize.remove();
        }
    }

    @Override
    protected AbstractBatchingProcessor newProcessor(String tag, String description, int batchSize, Map<String, Object> config) {
        String modelId = readStringProperty(TYPE, tag, config, MODEL_ID_FIELD);
        Map<String, Object> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
        boolean skipExisting = readBooleanProperty(TYPE, tag, config, SKIP_EXISTING, DEFAULT_SKIP_EXISTING);
        int batchSizeBytes = ConfigurationUtils.readIntProperty(TYPE, tag, config, BATCH_SIZE_BYTES_FIELD, DEFAULT_BATCH_SIZE_BYTES);
        batchSize = InferenceProcessor.validateBatchSizeBytesAndResolveEffectiveBatchSize(
            TYPE,
            tag,
            batchSizeBytes,
            batchSize,
            Boolean.TRUE.equals(userSetBatchSize.get())
        );
        return new TextEmbeddingProcessor(
            tag,
            description,
            batchSize,
            batchSizeBytes,
            modelId,
            fieldMap,
            skipExisting,
            skipExisting ? new TextEmbeddingInferenceFilter(fieldMap) : null,
            openSearchClient,
            clientAccessor,
            environment,
            clusterService
        );
    }
}
