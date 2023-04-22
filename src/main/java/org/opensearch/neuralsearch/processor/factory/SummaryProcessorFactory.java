/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.List;
import java.util.Map;

import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SummaryProcessor;
import org.opensearch.search.pipeline.Processor;

/**
 * A Factory class for creating {@link SummaryProcessor}
 */
public class SummaryProcessorFactory implements Processor.Factory {
    private final MLCommonsClientAccessor clientAccessor;

    public SummaryProcessorFactory(final MLCommonsClientAccessor clientAccessor) {
        this.clientAccessor = clientAccessor;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> registry, String processorTag, String description, Map<String, Object> config) {
        final List<String> fields = ConfigurationUtils.readList(SummaryProcessor.TYPE, processorTag, config, "fields");
        final String modelId = readStringProperty(SummaryProcessor.TYPE, processorTag, config, "modelId");
        final String promptType = readOptionalStringProperty(SummaryProcessor.TYPE, processorTag, config, "prompt");
        return new SummaryProcessor(processorTag, description, clientAccessor, fields, modelId, promptType);
    }
}
