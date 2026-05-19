/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.processor;

import java.util.List;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfigResolver;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.batch.utils.HighlightResultApplier;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

/**
 * System-generated search response processor for batch semantic highlighting.
 * Walks the request's highlight DSL — including {@code inner_hits} — to collect
 * every {@code type: semantic} field, executes batch inference (paginated when
 * the row count exceeds {@code max_inference_batch_size}), and writes the model
 * output back into each hit's highlight fields.
 */
@Log4j2
public class SemanticHighlightingProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    private final boolean ignoreFailure;
    private final MLCommonsClientAccessor mlCommonsClientAccessor;
    private final HighlightContextBuilder contextBuilder;
    private final String tag;
    private final String description;

    public SemanticHighlightingProcessor(boolean ignoreFailure, MLCommonsClientAccessor mlCommonsClientAccessor) {
        this.ignoreFailure = ignoreFailure;
        this.mlCommonsClientAccessor = mlCommonsClientAccessor;
        this.contextBuilder = new HighlightContextBuilder();
        this.tag = SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG;
        this.description = SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException("Semantic highlighting processor requires async processing");
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
            HighlightConfig config = HighlightConfigResolver.resolve(request);

            if (!config.hasTargets()) {
                responseListener.onResponse(response);
                return;
            }

            if (config.getQueryText() == null) {
                log.debug("Skipping semantic highlighting: could not extract query text");
                responseListener.onResponse(response);
                return;
            }

            EventStatsManager.increment(EventStatName.SEMANTIC_HIGHLIGHTING_BATCH_REQUEST_COUNT);

            HighlightContext context = contextBuilder.build(config, response, startTime);
            if (context.isEmpty()) {
                log.debug("No valid hits/inner hits with text to highlight");
                responseListener.onResponse(response);
                return;
            }

            if (context.getModelId() == null) {
                responseListener.onFailure(
                    new IllegalArgumentException(
                        "options.model_id is required on a semantic highlight field when batch inference is enabled"
                    )
                );
                return;
            }

            HighlightResultApplier applier = new HighlightResultApplier();

            if (context.getRequests().size() <= context.getMaxBatchSize()) {
                processSingleBatch(context, applier, responseListener);
            } else {
                new BatchExecutor(context, applier, responseListener).execute();
            }
        } catch (Exception e) {
            log.error("Error in semantic highlighting processor", e);
            handleError(e, response, responseListener);
        }
    }

    private void processSingleBatch(
        HighlightContext context,
        HighlightResultApplier applier,
        ActionListener<SearchResponse> responseListener
    ) {
        long batchStartTime = System.currentTimeMillis();
        mlCommonsClientAccessor.batchInferenceSentenceHighlighting(
            context.getModelId(),
            context.getRequests(),
            FunctionName.REMOTE,
            ActionListener.wrap(batchResults -> {
                try {
                    log.debug("Single batch completed: {} docs in {}ms", context.size(), System.currentTimeMillis() - batchStartTime);
                    applier.applyBatchResults(
                        context.getValidHits(),
                        batchResults,
                        context.getFieldNames(),
                        context.getPreTags(),
                        context.getPostTags(),
                        context.getNoMatchSizes(),
                        context.getEncoders()
                    );
                    completeProcessing(context, responseListener);
                } catch (Exception e) {
                    handleError(e, context.getOriginalResponse(), responseListener);
                }
            }, err -> handleError(err, context.getOriginalResponse(), responseListener))
        );
    }

    private void completeProcessing(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        long totalTime = System.currentTimeMillis() - context.getStartTime();
        SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(context.getOriginalResponse(), totalTime);
        responseListener.onResponse(finalResponse);
    }

    private void handleError(Throwable e, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn("Semantic highlighting failed; returning original response", e);
            responseListener.onResponse(response);
        } else if (e instanceof Exception) {
            responseListener.onFailure((Exception) e);
        } else {
            responseListener.onFailure(new RuntimeException(e));
        }
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
        // Run after user-defined processors so users can shape the response first.
        return ExecutionStage.POST_USER_DEFINED;
    }

    /** Serial pagination for context sizes that exceed {@code max_inference_batch_size}. */
    private class BatchExecutor {
        private final HighlightContext context;
        private final HighlightResultApplier applier;
        private final ActionListener<SearchResponse> responseListener;
        private final List<SentenceHighlightingRequest> allRequests;
        private int currentIndex = 0;

        BatchExecutor(HighlightContext context, HighlightResultApplier applier, ActionListener<SearchResponse> responseListener) {
            this.context = context;
            this.applier = applier;
            this.responseListener = responseListener;
            this.allRequests = context.getRequests();
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
            int endIdx = Math.min(startIdx + context.getMaxBatchSize(), allRequests.size());

            List<SentenceHighlightingRequest> slice = allRequests.subList(startIdx, endIdx);
            long batchStart = System.currentTimeMillis();

            mlCommonsClientAccessor.batchInferenceSentenceHighlighting(
                context.getModelId(),
                slice,
                FunctionName.REMOTE,
                ActionListener.wrap(batchResults -> {
                    try {
                        log.debug("Batch [{}, {}) completed in {}ms", startIdx, endIdx, System.currentTimeMillis() - batchStart);
                        applier.applyBatchResultsWithIndices(
                            context.getValidHits(),
                            batchResults,
                            startIdx,
                            endIdx,
                            context.getFieldNames(),
                            context.getPreTags(),
                            context.getPostTags(),
                            context.getNoMatchSizes(),
                            context.getEncoders()
                        );
                        currentIndex = endIdx;
                        processNextBatch();
                    } catch (Exception e) {
                        handleError(e, context.getOriginalResponse(), responseListener);
                    }
                }, err -> handleError(err, context.getOriginalResponse(), responseListener))
            );
        }
    }
}
