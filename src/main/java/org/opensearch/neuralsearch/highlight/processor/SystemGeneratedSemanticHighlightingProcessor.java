/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightRequestPreparer;
import org.opensearch.neuralsearch.highlight.HighlightRequestValidator;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
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
public class SystemGeneratedSemanticHighlightingProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    private final String tag;
    private final String description;
    private final boolean ignoreFailure;
    private final MLCommonsClientAccessor mlClientAccessor;
    private final HighlightRequestValidator validator;
    private final HighlightRequestPreparer preparer;

    public SystemGeneratedSemanticHighlightingProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        MLCommonsClientAccessor mlClientAccessor
    ) {
        this.tag = tag;
        this.description = description;
        this.ignoreFailure = ignoreFailure;
        this.mlClientAccessor = mlClientAccessor;
        this.validator = new HighlightRequestValidator();
        this.preparer = new HighlightRequestPreparer();
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
            // Extract model_id and configuration from query-level options
            HighlightRequestValidator.ValidationResult validation = validator.validate(
                request,
                response,
                null,
                SemanticHighlightingConstants.DEFAULT_PRE_TAG,
                SemanticHighlightingConstants.DEFAULT_POST_TAG
            );

            if (!validation.isValid()) {
                log.debug("Semantic highlighting validation failed: {}", validation.getErrorMessage());
                responseListener.onResponse(response);
                return;
            }

            // Get query-level configuration
            String modelId = validation.getModelId();
            boolean batchInference = validation.isBatchInference();
            int maxBatchSize = validation.getMaxBatchSize();
            String preTag = validation.getPreTag();
            String postTag = validation.getPostTag();

            // Prepare highlighting context
            HighlightContext context = preparer.prepare(
                response,
                validation.getQueryText(),
                validation.getSemanticField(),
                modelId,
                startTime,
                preTag,
                postTag
            );

            if (context.isEmpty()) {
                log.debug("No valid documents to highlight");
                responseListener.onResponse(response);
                return;
            }

            // Create appropriate strategy based on query configuration
            HighlightResultApplier applier = new HighlightResultApplier(preTag, postTag);
            HighlightingStrategy strategy;

            if (batchInference) {
                strategy = new BatchHighlighter(modelId, mlClientAccessor, maxBatchSize, applier, ignoreFailure);
                log.debug("Using BatchHighlighter with max batch size: {}", maxBatchSize);
            } else {
                strategy = new SingleHighlighter(mlClientAccessor, applier, ignoreFailure);
                log.debug("Using SingleHighlighter for backward compatibility");
            }

            // Execute highlighting
            strategy.process(context, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse highlightedResponse) {
                    responseListener.onResponse(highlightedResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ignoreFailure) {
                        log.warn("Semantic highlighting failed, returning original response", e);
                        responseListener.onResponse(response);
                    } else {
                        responseListener.onFailure(e);
                    }
                }
            });

        } catch (Exception e) {
            log.error("Error in semantic highlighting processor", e);
            if (ignoreFailure) {
                responseListener.onResponse(response);
            } else {
                responseListener.onFailure(e);
            }
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
