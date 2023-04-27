/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.opensearch.core.ParseField;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.GenerativeTextLLMProcessor;
import org.opensearch.search.pipeline.Processor;

/**
 * A Factory class for creating {@link GenerativeTextLLMProcessor}
 */
public class GenerativeTextLLMProcessorFactory implements Processor.Factory {

    private static final ParseField MODEL_ID = new ParseField("modelId");
    private static final ParseField USE_CASE = new ParseField("usecase");

    private final MLCommonsClientAccessor clientAccessor;

    public GenerativeTextLLMProcessorFactory(final MLCommonsClientAccessor clientAccessor) {
        this.clientAccessor = clientAccessor;
    }

    @Override
    public Processor create(Map<String, Processor.Factory> registry, String processorTag, String description, Map<String, Object> config) {
        final List<String> fields = ConfigurationUtils.readList(
            GenerativeTextLLMProcessor.TYPE,
            processorTag,
            config,
            ParseField.CommonFields.FIELDS.getPreferredName()
        );
        final String modelId = readStringProperty(GenerativeTextLLMProcessor.TYPE, processorTag, config, MODEL_ID.getPreferredName());
        final String usecase = readOptionalStringProperty(
            GenerativeTextLLMProcessor.TYPE,
            processorTag,
            config,
            USE_CASE.getPreferredName()
        );
        final String tag = StringUtils.isEmpty(processorTag) ? modelId : processorTag;
        return new GenerativeTextLLMProcessor(tag, description, clientAccessor, fields, modelId, usecase);
    }
}
