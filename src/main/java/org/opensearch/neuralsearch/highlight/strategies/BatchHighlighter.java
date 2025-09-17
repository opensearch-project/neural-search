/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.strategies;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.highlight.HighlightingStrategy;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.SearchHit;

import java.util.List;

/**
 * Batch highlighting strategy that processes multiple documents in batches
 */
@Log4j2
@RequiredArgsConstructor
public class BatchHighlighter implements HighlightingStrategy {

    private final String modelId;
    private final MLCommonsClientAccessor mlClient;
    private final int maxBatchSize;
    private final HighlightResultApplier resultApplier;
    private final boolean ignoreFailure;

    @Override
    public void process(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        List<SentenceHighlightingRequest> requests = context.getRequests();

        if (context.isEmpty()) {
            responseListener.onResponse(context.getOriginalResponse());
            return;
        }

        int totalRequests = requests.size();
        int numBatches = (totalRequests + maxBatchSize - 1) / maxBatchSize;

        log.info(
            "Starting batch semantic highlighting: {} documents in {} batch(es) of max size {}",
            totalRequests,
            numBatches,
            maxBatchSize
        );

        if (requests.size() <= maxBatchSize) {
            processSingleBatch(context, responseListener);
        } else {
            processMultipleBatches(context, responseListener);
        }
    }

    private void processSingleBatch(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        long batchStartTime = System.currentTimeMillis();

        mlClient.batchInferenceSentenceHighlighting(
            modelId,
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
                    handleError(e, context, responseListener);
                }
            }, error -> handleError(error, context, responseListener))
        );
    }

    private void processMultipleBatches(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        BatchExecutor executor = new BatchExecutor(context, responseListener);
        executor.execute();
    }

    private void completeProcessing(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        long totalTime = System.currentTimeMillis() - context.getStartTime();
        SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(context.getOriginalResponse(), totalTime);

        responseListener.onResponse(finalResponse);
    }

    private void handleError(Exception error, HighlightContext context, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn("Batch highlighting failed but ignoring failure: {}", error.getMessage());
            responseListener.onResponse(context.getOriginalResponse());
        } else {
            responseListener.onFailure(error);
        }
    }

    /**
     * Inner class to handle multi-batch execution
     */
    private class BatchExecutor {
        private final HighlightContext context;
        private final ActionListener<SearchResponse> responseListener;
        private final List<SentenceHighlightingRequest> allRequests;
        private final List<SearchHit> allValidHits;
        private int currentIndex = 0;

        BatchExecutor(HighlightContext context, ActionListener<SearchResponse> responseListener) {
            this.context = context;
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
            int endIdx = Math.min(startIdx + maxBatchSize, allRequests.size());
            int batchNumber = (startIdx / maxBatchSize) + 1;
            int totalBatches = (allRequests.size() + maxBatchSize - 1) / maxBatchSize;

            List<SentenceHighlightingRequest> batchRequests = allRequests.subList(startIdx, endIdx);

            log.debug("Processing batch {}/{}: documents {}-{}", batchNumber, totalBatches, startIdx + 1, endIdx);

            long batchStartTime = System.currentTimeMillis();

            mlClient.batchInferenceSentenceHighlighting(
                modelId,
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
                        handleError(e, context, responseListener);
                    }
                }, error -> handleError(error, context, responseListener))
            );
        }
    }
}
