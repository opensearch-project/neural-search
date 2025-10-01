/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.batch.utils.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.utils.HighlightConfigBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.List;

/**
 * System-generated processor that handles batch semantic highlighting.
 * This processor only handles batch inference mode since non-batch is handled by SemanticHighlighter directly.
 */
@Log4j2
public class SemanticHighlightingProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    private final boolean ignoreFailure;
    private final MLCommonsClientAccessor mlClientAccessor;
    private final HighlightContextBuilder contextBuilder;
    private final String tag;
    private final String description;

    public SemanticHighlightingProcessor(boolean ignoreFailure, MLCommonsClientAccessor mlClientAccessor) {
        this.ignoreFailure = ignoreFailure;
        this.mlClientAccessor = mlClientAccessor;
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
        // Increment batch stat
        EventStatsManager.increment(EventStatName.SEMANTIC_HIGHLIGHTING_BATCH_REQUEST_COUNT);

        try {
            // Use unified config builder that includes extraction and validation
            HighlightConfig config = HighlightConfigBuilder.buildFromSearchRequest(request, response);

            if (config.getValidationError() != null) {
                log.debug("Configuration extraction/validation failed: {}", config.getValidationError());
                responseListener.onResponse(response);
                return;
            }

            // Check if basic fields are present
            if (!config.hasRequiredFields()) {
                log.debug("Missing required fields for semantic highlighting");
                responseListener.onResponse(response);
                return;
            }

            // This processor only handles batch inference
            if (!config.isBatchInference()) {
                log.debug("Non-batch inference should be handled by SemanticHighlighter directly");
                responseListener.onResponse(response);
                return;
            }

            // For batch inference, always use REMOTE model type
            HighlightConfig enrichedConfig = config.withModelType(FunctionName.REMOTE);

            // Validate batch inference
            String batchValidationError = enrichedConfig.validateBatchInference();
            if (batchValidationError != null) {
                responseListener.onFailure(new IllegalArgumentException(batchValidationError));
                return;
            }

            if (!enrichedConfig.isValid()) {
                responseListener.onFailure(new IllegalArgumentException(enrichedConfig.getValidationError()));
                return;
            }

            // Build context and execute batch highlighting
            executeBatchHighlighting(enrichedConfig, response, startTime, responseListener);

        } catch (Exception e) {
            log.error("Error in semantic highlighting processor", e);
            handleError(e, response, responseListener);
        }
    }

    private void executeBatchHighlighting(
        HighlightConfig config,
        SearchResponse response,
        long startTime,
        ActionListener<SearchResponse> responseListener
    ) {
        HighlightContext context = contextBuilder.build(config, response, startTime);
        if (context.isEmpty()) {
            log.debug("No valid documents to highlight");
            responseListener.onResponse(response);
            return;
        }

        HighlightResultApplier resultApplier = new HighlightResultApplier(config.getPreTag(), config.getPostTag());

        if (context.getRequests().size() <= config.getMaxBatchSize()) {
            processSingleBatch(context, config, resultApplier, responseListener);
        } else {
            processMultipleBatches(context, config, resultApplier, responseListener);
        }
    }

    private void processSingleBatch(
        HighlightContext context,
        HighlightConfig config,
        HighlightResultApplier resultApplier,
        ActionListener<SearchResponse> responseListener
    ) {
        long batchStartTime = System.currentTimeMillis();

        mlClientAccessor.batchInferenceSentenceHighlighting(
            config.getModelId(),
            context.getRequests(),
            context.getModelType(),
            ActionListener.wrap(batchResults -> {
                try {
                    log.debug(
                        "Single batch inference completed: {} documents in {}ms",
                        context.size(),
                        System.currentTimeMillis() - batchStartTime
                    );

                    resultApplier.applyBatchResults(
                        context.getValidHits(),
                        batchResults,
                        context.getFieldName(),
                        context.getPreTag(),
                        context.getPostTag()
                    );

                    completeProcessing(context, responseListener);
                } catch (Exception e) {
                    handleError(e, context.getOriginalResponse(), responseListener);
                }
            }, error -> handleError(error, context.getOriginalResponse(), responseListener))
        );
    }

    private void processMultipleBatches(
        HighlightContext context,
        HighlightConfig config,
        HighlightResultApplier resultApplier,
        ActionListener<SearchResponse> responseListener
    ) {
        BatchExecutor executor = new BatchExecutor(context, config, resultApplier, responseListener);
        executor.execute();
    }

    private void completeProcessing(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        long totalTime = System.currentTimeMillis() - context.getStartTime();
        SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(context.getOriginalResponse(), totalTime);
        responseListener.onResponse(finalResponse);
    }

    private void handleError(Exception e, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn("Semantic highlighting failed, returning original response", e);
            responseListener.onResponse(response);
        } else {
            responseListener.onFailure(e);
        }
    }

    /**
     * Inner class to handle multi-batch execution
     */
    private class BatchExecutor {
        private final HighlightContext context;
        private final HighlightConfig config;
        private final HighlightResultApplier resultApplier;
        private final ActionListener<SearchResponse> responseListener;
        private final List<SentenceHighlightingRequest> allRequests;
        private final List<SearchHit> allValidHits;
        private int currentIndex = 0;

        BatchExecutor(
            HighlightContext context,
            HighlightConfig config,
            HighlightResultApplier resultApplier,
            ActionListener<SearchResponse> responseListener
        ) {
            this.context = context;
            this.config = config;
            this.resultApplier = resultApplier;
            this.responseListener = responseListener;
            this.allRequests = context.getRequests();
            this.allValidHits = context.getValidHits();
        }

        void execute() {
            processNextBatch();
        }

        private void processNextBatch() {
            if (currentIndex >= allRequests.size()) {
                completeProcessing(context, responseListener);
                return;
            }

            int startIdx = currentIndex;
            int endIdx = Math.min(startIdx + config.getMaxBatchSize(), allRequests.size());
            int batchNumber = (startIdx / config.getMaxBatchSize()) + 1;
            int totalBatches = (allRequests.size() + config.getMaxBatchSize() - 1) / config.getMaxBatchSize();

            List<SentenceHighlightingRequest> batchRequests = allRequests.subList(startIdx, endIdx);

            log.debug("Processing batch {}/{}: documents {}-{}", batchNumber, totalBatches, startIdx + 1, endIdx);

            long batchStartTime = System.currentTimeMillis();

            mlClientAccessor.batchInferenceSentenceHighlighting(
                config.getModelId(),
                batchRequests,
                context.getModelType(),
                ActionListener.wrap(batchResults -> {
                    try {
                        log.debug(
                            "Batch {}/{} completed: {} documents in {}ms",
                            batchNumber,
                            totalBatches,
                            batchRequests.size(),
                            System.currentTimeMillis() - batchStartTime
                        );

                        resultApplier.applyBatchResultsWithIndices(
                            allValidHits,
                            batchResults,
                            startIdx,
                            endIdx,
                            context.getFieldName(),
                            context.getPreTag(),
                            context.getPostTag()
                        );

                        currentIndex = endIdx;
                        processNextBatch();
                    } catch (Exception e) {
                        handleError(e, context.getOriginalResponse(), responseListener);
                    }
                }, error -> handleError(error, context.getOriginalResponse(), responseListener))
            );
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
