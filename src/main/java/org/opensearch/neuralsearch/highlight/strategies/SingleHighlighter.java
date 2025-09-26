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
 * Single inference highlighting strategy for backward compatibility
 * Processes documents one by one using single inference API
 */
@Log4j2
@RequiredArgsConstructor
public class SingleHighlighter implements HighlightingStrategy {

    private final MLCommonsClientAccessor mlClient;
    private final HighlightResultApplier resultApplier;
    private final boolean ignoreFailure;

    @Override
    public void process(HighlightContext context, ActionListener<SearchResponse> responseListener) {
        if (context.isEmpty()) {
            responseListener.onResponse(context.getOriginalResponse());
            return;
        }

        log.info("Starting sequential semantic highlighting: {} documents", context.size());

        DocumentIterator iterator = new DocumentIterator(context, responseListener);
        iterator.processNext();
    }

    /**
     * Inner class to handle sequential document processing
     */
    private class DocumentIterator {
        private final HighlightContext context;
        private final ActionListener<SearchResponse> responseListener;
        private final List<SentenceHighlightingRequest> requests;
        private final List<SearchHit> validHits;
        private int currentIndex = 0;

        DocumentIterator(HighlightContext context, ActionListener<SearchResponse> responseListener) {
            this.context = context;
            this.responseListener = responseListener;
            this.requests = context.getRequests();
            this.validHits = context.getValidHits();
        }

        void processNext() {
            if (currentIndex >= requests.size()) {
                completeProcessing();
                return;
            }

            SentenceHighlightingRequest request = requests.get(currentIndex);
            SearchHit hit = validHits.get(currentIndex);

            log.debug("Processing document {}/{}", currentIndex + 1, requests.size());

            mlClient.inferenceSentenceHighlighting(request, context.getModelType(), ActionListener.wrap(highlightResults -> {
                try {
                    resultApplier.applySingleResult(
                        hit,
                        highlightResults,
                        context.getFieldName(),
                        context.getPreTag(),
                        context.getPostTag()
                    );
                    currentIndex++;
                    processNext();
                } catch (Exception e) {
                    handleError(e);
                }
            }, this::handleError));
        }

        private void completeProcessing() {
            long totalTime = System.currentTimeMillis() - context.getStartTime();
            SearchResponse finalResponse = ProcessorUtils.updateResponseTookTime(context.getOriginalResponse(), totalTime);

            log.info("Sequential semantic highlighting completed: {} documents in {}ms", requests.size(), totalTime);

            responseListener.onResponse(finalResponse);
        }

        private void handleError(Exception error) {
            if (ignoreFailure) {
                log.warn("Single highlighting failed but ignoring failure: {}", error.getMessage());
                responseListener.onResponse(context.getOriginalResponse());
            } else {
                responseListener.onFailure(error);
            }
        }
    }
}
