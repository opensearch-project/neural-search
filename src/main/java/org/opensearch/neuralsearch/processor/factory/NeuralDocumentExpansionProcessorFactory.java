/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.NeuralSparseDocumentProcessor.*;

import java.util.Map;

import org.opensearch.env.Environment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsNeuralSparseClientAccessor;
import org.opensearch.neuralsearch.processor.NeuralSparseDocumentProcessor;

public class NeuralDocumentExpansionProcessorFactory implements Processor.Factory {
    private final MLCommonsNeuralSparseClientAccessor clientAccessor;

    private final Environment environment;

    public NeuralDocumentExpansionProcessorFactory(MLCommonsNeuralSparseClientAccessor clientAccessor, Environment environment) {
        this.clientAccessor = clientAccessor;
        this.environment = environment;
    }

    @Override
    public NeuralSparseDocumentProcessor create(
        Map<String, Processor.Factory> registry,
        String processorTag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        String modelId = readStringProperty(TYPE, processorTag, config, MODEL_ID_FIELD);
        Map<String, Object> filedMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
        return new NeuralSparseDocumentProcessor(processorTag, description, modelId, filedMap, clientAccessor, environment);
    }
}
