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
import org.opensearch.neuralsearch.highlight.HighlightingStrategy;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.strategies.BatchHighlighter;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.highlight.HighlightRequestPreparer;
import org.opensearch.neuralsearch.highlight.HighlightRequestValidator;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Locale;

import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.PROCESSOR_TYPE;

/**
 * Semantic highlighting response processor
 */
@Log4j2
public class SemanticHighlightingResponseProcessor implements SearchResponseProcessor {

    private final String tag;
    private final String description;
    private final boolean ignoreFailure;
    private final String modelId;
    private final String preTag;
    private final String postTag;
    private final HighlightRequestValidator validator;
    private final HighlightRequestPreparer preparer;
    private final HighlightingStrategy defaultStrategy;
    private final MLCommonsClientAccessor mlClientAccessor;
    private final boolean pipelineBatchInference;
    private final int pipelineMaxBatchSize;

    public SemanticHighlightingResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String modelId,
        String preTag,
        String postTag,
        HighlightRequestValidator validator,
        HighlightRequestPreparer preparer,
        HighlightingStrategy defaultStrategy,
        MLCommonsClientAccessor mlClientAccessor,
        boolean pipelineBatchInference,
        int pipelineMaxBatchSize
    ) {
        this.tag = tag;
        this.description = description;
        this.ignoreFailure = ignoreFailure;
        this.modelId = modelId;
        this.preTag = preTag;
        this.postTag = postTag;
        this.validator = validator;
        this.preparer = preparer;
        this.defaultStrategy = defaultStrategy;
        this.mlClientAccessor = mlClientAccessor;
        this.pipelineBatchInference = pipelineBatchInference;
        this.pipelineMaxBatchSize = pipelineMaxBatchSize;
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
            // Validate request and extract necessary information
            HighlightRequestValidator.ValidationResult validation = validator.validate(request, response, modelId, preTag, postTag);

            if (!validation.isValid()) {
                log.debug("Validation failed: {}", validation.getErrorMessage());
                responseListener.onResponse(response);
                return;
            }

            // Prepare highlighting context
            HighlightContext context = preparer.prepare(
                response,
                validation.getQueryText(),
                validation.getSemanticField(),
                validation.getModelId(),
                startTime,
                validation.getPreTag(),
                validation.getPostTag()
            );

            if (context.isEmpty()) {
                log.debug("No valid documents to highlight");
                responseListener.onResponse(response);
                return;
            }

            // Select strategy based on query-level configuration or pipeline default
            HighlightingStrategy selectedStrategy = selectStrategy(validation);

            // Execute highlighting strategy
            selectedStrategy.process(context, wrapListener(responseListener, response, startTime));

        } catch (Exception e) {
            handleError(e, response, responseListener);
        }
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException(String.format(Locale.ROOT, "%s processor requires async processing", PROCESSOR_TYPE));
    }

    @Override
    public String getType() {
        return PROCESSOR_TYPE;
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

    /**
     * Select the appropriate highlighting strategy based on configuration
     */
    private HighlightingStrategy selectStrategy(HighlightRequestValidator.ValidationResult validation) {
        // Query-level configuration takes precedence over pipeline configuration
        boolean useBatch = validation.isBatchInference();
        int maxBatchSize = validation.getMaxBatchSize();

        // If query doesn't specify batch_inference, use pipeline default
        if (!useBatch && pipelineBatchInference) {
            useBatch = true;
            maxBatchSize = pipelineMaxBatchSize;
        }

        if (useBatch) {
            // Create batch highlighter with query-specific or pipeline batch size
            HighlightResultApplier applier = new HighlightResultApplier(validation.getPreTag(), validation.getPostTag());
            log.debug("Using BatchHighlighter with batch size: {}", maxBatchSize);
            return new BatchHighlighter(validation.getModelId(), mlClientAccessor, maxBatchSize, applier, ignoreFailure);
        } else {
            // Use single highlighter for backward compatibility
            log.debug("Using SingleHighlighter");
            return defaultStrategy;
        }
    }

    /**
     * Wrap response listener with metrics tracking
     */
    private ActionListener<SearchResponse> wrapListener(
        ActionListener<SearchResponse> originalListener,
        SearchResponse fallbackResponse,
        long startTime
    ) {
        return ActionListener.wrap(response -> {
            long totalTime = System.currentTimeMillis() - startTime;
            SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(response, totalTime);
            originalListener.onResponse(finalResponse);
        }, error -> handleError(error, fallbackResponse, originalListener));
    }

    /**
     * Handle errors based on ignore failure configuration
     */
    private void handleError(Exception error, SearchResponse response, ActionListener<SearchResponse> listener) {
        if (ignoreFailure) {
            log.warn("Semantic highlighting failed but ignoring failure: {}", error.getMessage());
            listener.onResponse(response);
        } else {
            listener.onFailure(error);
        }
    }
}
