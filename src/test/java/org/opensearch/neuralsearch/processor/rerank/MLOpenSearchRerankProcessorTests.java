/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.rerank.context.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.QueryContextSourceFetcher;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class MLOpenSearchRerankProcessorTests extends OpenSearchTestCase {

    @Mock
    private SearchRequest request;

    private SearchResponse response;

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private PipelineContext pipelineContext;

    @Mock
    private PipelineProcessingContext ppctx;

    @Mock
    private ClusterService clusterService;

    private RerankProcessorFactory factory;

    private MLOpenSearchRerankProcessor processor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        doReturn(Settings.EMPTY).when(clusterService).getSettings();
        factory = new RerankProcessorFactory(mlCommonsClientAccessor, clusterService);
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>(List.of("text_representation"))))
            )
        );
        processor = (MLOpenSearchRerankProcessor) factory.create(
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
        NeuralQueryBuilder nqb = NeuralQueryBuilder.builder()
            .fieldName("embedding")
            .k(3)
            .modelId("embedding_id")
            .queryText("Question about dolphins")
            .build();
        ssb.query(nqb);
        List<SearchExtBuilder> exts = List.of(
            new RerankSearchExtBuilder(new HashMap<>(Map.of(QueryContextSourceFetcher.NAME, new HashMap<>(params))))
        );
        ssb.ext(exts);
        doReturn(ssb).when(request).source();
    }

    private void setupSimilarityRescoring() {
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(1);
            List<Float> scores = List.of(1f, 2f, 3f);
            listener.onResponse(scores);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSimilarity(
                argThat(request -> request.getQueryText() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );
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
        TotalHits totalHits = new TotalHits(3, TotalHits.Relation.EQUAL_TO);

        SearchHits searchHits = new SearchHits(hitArray, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
    }

    public void testRerankContext_whenQueryText_thenSucceed() throws IOException {
        NeuralSearchClusterTestUtils.setUpClusterService();
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text"));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> argCaptor = ArgumentCaptor.forClass(Map.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().containsKey(QueryContextSourceFetcher.QUERY_TEXT_FIELD));
        assert (argCaptor.getValue().get(QueryContextSourceFetcher.QUERY_TEXT_FIELD).equals("query text"));
    }

    public void testRerankContext_whenQueryTextPath_thenSucceed() throws IOException {
        NeuralSearchClusterTestUtils.setUpClusterService();
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD, "query.neural.embedding.query_text"));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> argCaptor = ArgumentCaptor.forClass(Map.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().containsKey(QueryContextSourceFetcher.QUERY_TEXT_FIELD));
        assert (argCaptor.getValue().get(QueryContextSourceFetcher.QUERY_TEXT_FIELD).equals("Question about dolphins"));
    }

    public void testRerankContext_whenQueryTextAndPath_thenFail() throws IOException {
        NeuralSearchClusterTestUtils.setUpClusterService();
        setupParams(
            Map.of(
                QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD,
                "query.neural.embedding.query_text",
                QueryContextSourceFetcher.QUERY_TEXT_FIELD,
                "query text"
            )
        );
        setupSearchResults();
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
                    + QueryContextSourceFetcher.QUERY_TEXT_FIELD
                    + "\" and \""
                    + QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD
                    + "\""
            ));
    }

    public void testRerankContext_whenNoQueryInfo_thenFail() throws IOException {
        setupParams(Map.of());
        setupSearchResults();
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
                    + QueryContextSourceFetcher.QUERY_TEXT_FIELD
                    + "\" or \""
                    + QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD
                    + "\""
            ));
    }

    public void testRerankContext_whenQueryTextPathIsBadPointer_thenFail() throws IOException {
        NeuralSearchClusterTestUtils.setUpClusterService();
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD, "query.neural.embedding"));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD + " must point to a string field"));
    }

    @SneakyThrows
    public void testRerankContext_whenQueryTextPathIsExceeedinglyManyCharacters_thenFail() {
        NeuralSearchClusterTestUtils.setUpClusterService();
        // "eighteencharacters" * 60 = 1080 character string > max len of 1024
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD, "eighteencharacters".repeat(60)));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(
                String.format(
                    Locale.ROOT,
                    "%s exceeded the maximum path length of %d characters",
                    QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD,
                    QueryContextSourceFetcher.MAX_QUERY_PATH_STRLEN
                )
            ));
    }

    @SneakyThrows
    public void textRerankContext_whenQueryTextPathIsExceeedinglyDeeplyNested_thenFail() {
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD, "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.w.x.y.z"));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        processor.generateRerankingContext(request, response, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalArgumentException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(
                String.format(
                    Locale.ROOT,
                    "%s exceeded the maximum path length of %d nested fields",
                    QueryContextSourceFetcher.QUERY_TEXT_PATH_FIELD,
                    MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(clusterService.getSettings())
                )
            ));
    }

    public void testRescoreSearchResponse_HappyPath() throws IOException {
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(
            QueryContextSourceFetcher.QUERY_TEXT_FIELD,
            "query text",
            DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD,
            new ArrayList<>(List.of("dummy", "dummy", "dummy"))
        );
        processor.rescoreSearchResponse(response, scoringContext, listener);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Float>> argCaptor = ArgumentCaptor.forClass(List.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        assert (argCaptor.getValue().size() == 3);
        assert (argCaptor.getValue().get(0) == 1f);
        assert (argCaptor.getValue().get(1) == 2f);
        assert (argCaptor.getValue().get(2) == 3f);
    }

    public void testRescoreSearchResponse_whenNoContextList_thenFail() throws IOException {
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");
        processor.rescoreSearchResponse(response, scoringContext, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assert (argCaptor.getValue() instanceof IllegalStateException);
        assert (argCaptor.getValue()
            .getMessage()
            .equals(
                String.format(
                    Locale.ROOT,
                    "No document context found! Perhaps \"%s.%s\" is missing from the pipeline definition?",
                    RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                    DocumentContextSourceFetcher.NAME
                )
            ));
    }

    public void testRerank_HappyPath() throws IOException {
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(
            QueryContextSourceFetcher.QUERY_TEXT_FIELD,
            "query text",
            DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD,
            new ArrayList<>(List.of("dummy", "dummy", "dummy"))
        );
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

    public void testRerank_whenScoresAndHitsHaveDiffLengths_thenFail() throws IOException {
        doAnswer(invocation -> {
            ActionListener<List<Float>> listener = invocation.getArgument(1);
            List<Float> scores = List.of(1f, 2f);
            listener.onResponse(scores);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSimilarity(
                argThat(request -> request.getQueryText() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        Map<String, Object> scoringContext = Map.of(
            QueryContextSourceFetcher.QUERY_TEXT_FIELD,
            "query text",
            DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD,
            new ArrayList<>(List.of("dummy", "dummy", "dummy"))
        );
        processor.rerank(response, scoringContext, listener);
        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assertEquals(argCaptor.getValue().getMessage(), "scores and hits are not the same length");
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
        NeuralSearchClusterTestUtils.setUpClusterService();
        setupParams(Map.of(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text"));
        setupSimilarityRescoring();
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.processResponseAsync(request, response, ppctx, listener);
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
