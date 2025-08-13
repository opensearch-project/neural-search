/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.highlight.HighlightingStrategy;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.HighlightRequestPreparer;
import org.opensearch.neuralsearch.highlight.HighlightRequestValidator;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.strategies.BatchHighlighter;
import org.opensearch.neuralsearch.highlight.strategies.SingleHighlighter;
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
        int maxInferenceBatchSize = ConfigurationUtils.readIntProperty(
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            processorTag,
            config,
            SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE,
            SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE
        );

        // Create core components with default tags (query-level will override)
        HighlightRequestValidator validator = new HighlightRequestValidator();
        HighlightRequestPreparer preparer = new HighlightRequestPreparer();
        HighlightResultApplier applier = new HighlightResultApplier(
            SemanticHighlightingConstants.DEFAULT_PRE_TAG,
            SemanticHighlightingConstants.DEFAULT_POST_TAG
        );

        // Select strategy based on configuration
        HighlightingStrategy strategy;
        if (batchInference) {
            strategy = new BatchHighlighter(modelId, mlClientAccessor, maxInferenceBatchSize, applier, ignoreFailure);
            log.info("Created BatchHighlighter with max batch size: {}", maxInferenceBatchSize);
        } else {
            strategy = new SingleHighlighter(mlClientAccessor, applier, ignoreFailure);
            log.info("Created SingleHighlighter for backward compatibility");
        }

        return new SemanticHighlightingResponseProcessor(
            processorTag,
            description,
            ignoreFailure,
            modelId,
            SemanticHighlightingConstants.DEFAULT_PRE_TAG,
            SemanticHighlightingConstants.DEFAULT_POST_TAG,
            validator,
            preparer,
            strategy,
            mlClientAccessor,
            batchInference,
            maxInferenceBatchSize
        );
    }
}
