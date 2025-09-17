/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.highlight.HighlightConfig;
import org.opensearch.neuralsearch.highlight.HighlightConfigExtractor;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.HighlightValidator;
import org.opensearch.neuralsearch.highlight.HighlightingStrategy;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.strategies.BatchHighlighter;
import org.opensearch.neuralsearch.highlight.strategies.SingleHighlighter;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

/**
 * System-generated processor that handles semantic highlighting.
 * Automatically applied when semantic highlighting is detected in search queries.
 * This processor extracts all configuration from query-level options rather than
 * storing configuration at pipeline creation time.
 */
@Log4j2
public class SemanticHighlightingProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    private final boolean ignoreFailure;
    private final MLCommonsClientAccessor mlClientAccessor;
    private final HighlightConfigExtractor configExtractor;
    private final HighlightValidator validator;
    private final HighlightContextBuilder contextBuilder;
    private final String tag;
    private final String description;

    public SemanticHighlightingProcessor(boolean ignoreFailure, MLCommonsClientAccessor mlClientAccessor) {
        this.ignoreFailure = ignoreFailure;
        this.mlClientAccessor = mlClientAccessor;
        this.configExtractor = new HighlightConfigExtractor();
        this.validator = new HighlightValidator();
        this.contextBuilder = new HighlightContextBuilder();
        this.tag = SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG;
        this.description = SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION;
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext responseContext,
        ActionListener<SearchResponse> responseListener
    ) {
        long startTime = System.currentTimeMillis();

        try {
            HighlightConfig config = configExtractor.extract(request, response);

            if (config.getValidationError() != null) {
                log.debug("Configuration extraction failed: {}", config.getValidationError());
                responseListener.onResponse(response);
                return;
            }

            config = validator.validate(config, response);
            if (!config.isValid()) {
                log.debug("Validation failed: {}", config.getValidationError());
                responseListener.onResponse(response);
                return;
            }

            HighlightContext context = contextBuilder.build(config, response, startTime);
            if (context.isEmpty()) {
                log.debug("No valid documents to highlight");
                responseListener.onResponse(response);
                return;
            }

            // Select and create appropriate strategy
            HighlightingStrategy strategy = createStrategy(config);
            log.debug(
                "Using {} for highlighting with model: {}",
                config.isBatchInference() ? "BatchHighlighter" : "SingleHighlighter",
                config.getModelId()
            );

            // Execute highlighting
            strategy.process(context, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse highlightedResponse) {
                    responseListener.onResponse(highlightedResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    handleError(e, response, responseListener);
                }
            });

        } catch (Exception e) {
            log.error("Error in semantic highlighting processor", e);
            handleError(e, response, responseListener);
        }
    }

    private HighlightingStrategy createStrategy(HighlightConfig config) {
        HighlightResultApplier applier = new HighlightResultApplier(config.getPreTag(), config.getPostTag());

        if (config.isBatchInference()) {
            return new BatchHighlighter(config.getModelId(), mlClientAccessor, config.getMaxBatchSize(), applier, ignoreFailure);
        }

        return new SingleHighlighter(mlClientAccessor, applier, ignoreFailure);
    }

    private void handleError(Exception e, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn("Semantic highlighting failed, returning original response", e);
            responseListener.onResponse(response);
        } else {
            responseListener.onFailure(e);
        }
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException("Semantic highlighting processor requires async processing");
    }

    @Override
    public String getType() {
        return SemanticHighlightingConstants.PROCESSOR_TYPE;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    @Override
    public SystemGeneratedProcessor.ExecutionStage getExecutionStage() {
        // Execute after user-defined processors to allow them to modify the response first
        return ExecutionStage.POST_USER_DEFINED;
    }
}
