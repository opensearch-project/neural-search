/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.*;

import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.opensearch.env.Environment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;

/**
 * Factory for sparse encoding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
@Log4j2
public class SparseEncodingProcessorFactory implements Processor.Factory {
    private final MLCommonsClientAccessor clientAccessor;
    private final Environment environment;

    public SparseEncodingProcessorFactory(MLCommonsClientAccessor clientAccessor, Environment environment) {
        this.clientAccessor = clientAccessor;
        this.environment = environment;
    }

    @Override
    public SparseEncodingProcessor create(
        Map<String, Processor.Factory> registry,
        String processorTag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        String modelId = readStringProperty(TYPE, processorTag, config, MODEL_ID_FIELD);
        Map<String, Object> filedMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);

        return new SparseEncodingProcessor(processorTag, description, modelId, filedMap, clientAccessor, environment);
    }
}
