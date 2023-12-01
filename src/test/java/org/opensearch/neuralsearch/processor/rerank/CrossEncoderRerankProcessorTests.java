/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.rerank;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.test.OpenSearchTestCase;

@Log4j2
public class CrossEncoderRerankProcessorTests extends OpenSearchTestCase {

    @Mock
    SearchRequest request;

    SearchResponse response;

    @Mock
    MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    PipelineContext pipelineContext;

    RerankProcessorFactory factory;

    CrossEncoderRerankProcessor processor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        factory = new RerankProcessorFactory(mlCommonsClientAccessor);
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.CROSS_ENCODER.getLabel(),
                new HashMap<>(
                    Map.of(
                        CrossEncoderRerankProcessor.MODEL_ID_FIELD,
                        "model-id",
                        CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD,
                        "text_representation"
                    )
                )
            )
        );
        processor = (CrossEncoderRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for reranking with a cross encoder",
            false,
            config,
            pipelineContext
        );
    }

    private void setupParams(Map<String, Object> params) {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        NeuralQueryBuilder nqb = new NeuralQueryBuilder();
        nqb.fieldName("embedding").k(3).modelId("embedding_id").queryText("Question about dolphins");
        ssb.query(nqb);
        List<SearchExtBuilder> exts = List.of(new RerankSearchExtBuilder(new HashMap<>(params)));
        ssb.ext(exts);
        doReturn(ssb).when(request).source();
    }

    private void setupSimilarityRescoring() {
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(3);
            List<Float> scores = List.of(1f, 2f, 3f);
            listener.onResponse(scores);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(anyString(), anyString(), anyList(), any());
    }

    private void setupSearchResults() throws IOException {
        XContentBuilder sourceContent = JsonXContent.contentBuilder()
            .startObject()
            .field("text_representation", "source passage")
            .endObject();
        SearchHit sourceHit = new SearchHit(0, "0", Map.of(), Map.of());
        sourceHit.sourceRef(BytesReference.bytes(sourceContent));
        sourceHit.score(1.5f);

        DocumentField field = new DocumentField("text_representation", List.of("field passage"));
        SearchHit fieldHit = new SearchHit(1, "1", Map.of("text_representation", field), Map.of());
        fieldHit.score(1.7f);

        SearchHit nullHit = new SearchHit(2, "2", Map.of(), Map.of());
        nullHit.score(0f);

        SearchHit[] hitArray = new SearchHit[] { fieldHit, sourceHit, nullHit };

        SearchHits searchHits = new SearchHits(hitArray, null, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
    }

    public void testScoringContext_QueryText_ThenSucceed() {
        setupParams(Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD, "query text"));
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> argCaptor = ArgumentCaptor.forClass(Map.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().containsKey(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD));
        assert (argCaptor.getValue().get(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD).equals("query text"));
    }

    public void testScoringContext_QueryTextPath_ThenSucceed() {
        setupParams(Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD, "query.neural.embedding.query_text"));
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> argCaptor = ArgumentCaptor.forClass(Map.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().containsKey(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD));
        assert (argCaptor.getValue().get(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD).equals("Question about dolphins"));
    }

    public void testScoringContext_QueryTextAndPath_ThenFail() {
        setupParams(
            Map.of(
                CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD,
                "query.neural.embedding.query_text",
                CrossEncoderRerankProcessor.QUERY_TEXT_FIELD,
                "query text"
            )
        );
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(
                "Cannot specify both \""
                    + CrossEncoderRerankProcessor.QUERY_TEXT_FIELD
                    + "\" and \""
                    + CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD
                    + "\""
            ));
    }

    public void testScoringContext_NoQueryInfo_ThenFail() {
        setupParams(Map.of());
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(
                "Must specify either \""
                    + CrossEncoderRerankProcessor.QUERY_TEXT_FIELD
                    + "\" or \""
                    + CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD
                    + "\""
            ));
    }

    public void testScoringContext_QueryTextPath_BadPointer_ThenFail() {
        setupParams(Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD, "query.neural.embedding"));
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(CrossEncoderRerankProcessor.QUERY_TEXT_PATH_FIELD + " must point to a string field"));
    }

    public void testRescoreSearchResponse_HappyPath() throws IOException {
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD, "query text");
        processor.rescoreSearchResponse(response, scoringContext, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Float>> argCaptor = ArgumentCaptor.forClass(List.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().size() == 3);
        assert (argCaptor.getValue().get(0) == 1f);
        assert (argCaptor.getValue().get(1) == 2f);
        assert (argCaptor.getValue().get(2) == 3f);
    }

    public void testRerank_HappyPath() throws IOException {
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD, "query text");
        processor.rerank(response, scoringContext, listener);
        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse rsp = argCaptor.getValue();
        assert (rsp.getHits().getAt(0).docId() == 2);
        assert (rsp.getHits().getAt(0).getScore() == 3f);
        assert (rsp.getHits().getAt(1).docId() == 0);
        assert (rsp.getHits().getAt(1).getScore() == 2f);
        assert (rsp.getHits().getAt(2).docId() == 1);
        assert (rsp.getHits().getAt(2).getScore() == 1f);
    }

    public void testRerank_ScoresAndHitsHaveDiffLengths() throws IOException {
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(3);
            List<Float> scores = List.of(1f, 2f);
            listener.onResponse(scores);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(anyString(), anyString(), anyList(), any());
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD, "query text");
        processor.rerank(response, scoringContext, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue().getMessage().equals("scores and hits are not the same length"));
    }

    public void testBasics() throws IOException {
        assert (processor.getTag().equals("rerank processor"));
        assert (processor.getDescription().equals("processor for reranking with a cross encoder"));
        assert (!processor.isIgnoreFailure());
        assertThrows(
            "Use asyncProcessResponse unless you can guarantee to not deadlock yourself",
            UnsupportedOperationException.class,
            () -> processor.processResponse(request, response)
        );
    }

    public void testProcessResponseAsync() throws IOException {
        setupParams(Map.of(CrossEncoderRerankProcessor.QUERY_TEXT_FIELD, "query text"));
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.processResponseAsync(request, response, listener);
        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse rsp = argCaptor.getValue();
        assert (rsp.getHits().getAt(0).docId() == 2);
        assert (rsp.getHits().getAt(0).getScore() == 3f);
        assert (rsp.getHits().getAt(1).docId() == 0);
        assert (rsp.getHits().getAt(1).getScore() == 2f);
        assert (rsp.getHits().getAt(2).docId() == 1);
        assert (rsp.getHits().getAt(2).getScore() == 1f);
    }
}
