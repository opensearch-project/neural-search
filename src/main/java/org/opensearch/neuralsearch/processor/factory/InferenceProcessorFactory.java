/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.ingest.Processor.Factory;

import java.util.Map;

import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.InferenceProcessor;

public class InferenceProcessorFactory implements Factory {

    private final MLCommonsClientAccessor clientAccessor;

    private final Environment environment;

    public InferenceProcessorFactory(MLCommonsClientAccessor clientAccessor, Environment environment) {
        this.clientAccessor = clientAccessor;
        this.environment = environment;
    }

    @Override
    public InferenceProcessor create(Map<String, Factory> registry, String processorTag, String description, Map<String, Object> config)
        throws Exception {
        String modelId = readStringProperty(InferenceProcessor.TYPE, processorTag, config, InferenceProcessor.MODEL_ID_FIELD);
        Map<String, Object> filedMap = readMap(InferenceProcessor.TYPE, processorTag, config, InferenceProcessor.FIELD_MAP_FIELD);
        return new InferenceProcessor(processorTag, description, modelId, filedMap, clientAccessor, environment);
    }
}
