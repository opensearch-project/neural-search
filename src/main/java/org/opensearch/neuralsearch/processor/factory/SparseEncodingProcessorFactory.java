/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;

import java.util.Map;

import org.opensearch.env.Environment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;

import lombok.extern.log4j.Log4j2;

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
