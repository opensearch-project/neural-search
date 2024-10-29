/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class ProcessorExplainPublisherTests extends OpenSearchTestCase {
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    public void testClassFields_whenCreateNewObject_thenAllFieldsPresent() {
        ProcessorExplainPublisher processorExplainPublisher = new ProcessorExplainPublisher(DESCRIPTION, PROCESSOR_TAG, false);

        assertEquals(DESCRIPTION, processorExplainPublisher.getDescription());
        assertEquals(PROCESSOR_TAG, processorExplainPublisher.getTag());
        assertFalse(processorExplainPublisher.isIgnoreFailure());
    }

    @SneakyThrows
    public void testPipelineContext_whenPipelineContextHasNoExplanationInfo_thenProcessorIsNoOp() {
        ProcessorExplainPublisher processorExplainPublisher = new ProcessorExplainPublisher(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchResponse searchResponse = new SearchResponse(
            null,
            null,
            1,
            1,
            0,
            1000,
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY
        );

        SearchResponse processedResponse = processorExplainPublisher.processResponse(searchRequest, searchResponse);
        assertEquals(searchResponse, processedResponse);

        SearchResponse processedResponse2 = processorExplainPublisher.processResponse(searchRequest, searchResponse, null);
        assertEquals(searchResponse, processedResponse2);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        SearchResponse processedResponse3 = processorExplainPublisher.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );
        assertEquals(searchResponse, processedResponse3);
    }
}
