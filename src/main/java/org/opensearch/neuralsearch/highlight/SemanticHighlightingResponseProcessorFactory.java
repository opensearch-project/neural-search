/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.extern.log4j.Log4j2;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.io.IOException;
import java.util.Map;

/**
 * Factory class for creating SemanticHighlightingResponseProcessor
 */
@Log4j2
public class SemanticHighlightingResponseProcessorFactory implements Processor.Factory<SearchResponseProcessor> {
    private final MLCommonsClientAccessor mlClientAccessor;

    public SemanticHighlightingResponseProcessorFactory(MLCommonsClientAccessor mlClientAccessor) {
        this.mlClientAccessor = mlClientAccessor;
    }

    @Override
    public SemanticHighlightingResponseProcessor create(
        Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        String processorTag,
        String description,
        boolean ignoreFailure,
        Map<String, Object> config,
        Processor.PipelineContext pipelineContext
    ) throws IOException {
        // Required configuration
        String modelId = ConfigurationUtils.readStringProperty(
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            processorTag,
            config,
            SemanticHighlightingConstants.MODEL_ID
        );

        // Optional configuration with backward compatibility defaults
        boolean batchInference = ConfigurationUtils.readBooleanProperty(
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            processorTag,
            config,
            SemanticHighlightingConstants.BATCH_INFERENCE,
            false
        );
        String preTag = ConfigurationUtils.readStringProperty(
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            processorTag,
            config,
            SemanticHighlightingConstants.PRE_TAG,
            SemanticHighlightingConstants.DEFAULT_PRE_TAG
        );
        String postTag = ConfigurationUtils.readStringProperty(
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            processorTag,
            config,
            SemanticHighlightingConstants.POST_TAG,
            SemanticHighlightingConstants.DEFAULT_POST_TAG
        );

        return new SemanticHighlightingResponseProcessor(
            processorTag,
            description,
            ignoreFailure,
            modelId,
            mlClientAccessor,
            batchInference,
            preTag,
            postTag
        );
    }
}
