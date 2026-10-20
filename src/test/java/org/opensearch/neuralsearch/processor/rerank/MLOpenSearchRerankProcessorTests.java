/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.rerank.context.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.InnerHitsContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.QueryContextSourceFetcher;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.test.OpenSearchTestCase;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

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
        TestUtils.initializeEventStatsManager();
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
        setUpClusterService();
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

    @SneakyThrows
    public void testRerank_whenInnerHits_thenScoresChunksOnceAndSortsThem() {
        SearchHit parent0 = createParentHit(0, "0");
        Map<String, SearchHits> parent0Lists = new LinkedHashMap<>();
        parent0Lists.put("keyword_chunks", innerHitsList(createChunk(0, "alpha"), createChunk(1, "beta")));
        parent0Lists.put("semantic_chunks", innerHitsList(createChunk(1, "beta"), createChunk(2, "gamma")));
        parent0.setInnerHits(parent0Lists);

        SearchHit parent1 = createParentHit(1, "1");
        parent1.setInnerHits(new LinkedHashMap<>(Map.of("semantic_chunks", innerHitsList(createChunk(0, "delta")))));

        SearchHit parent2 = createParentHit(2, "2");

        TotalHits totalHits = new TotalHits(3, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent0, parent1, parent2 }, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);

        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.1f, 0.9f, 0.5f, 0.7f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));

        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<SimilarityInferenceRequest> requestCaptor = ArgumentCaptor.forClass(SimilarityInferenceRequest.class);
        verify(mlCommonsClientAccessor, times(1)).inferenceSimilarity(requestCaptor.capture(), isA(ActionListener.class));
        assertEquals(List.of("alpha", "beta", "gamma", "delta"), requestCaptor.getValue().getInputTexts());

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHits rerankedHits = argCaptor.getValue().getHits();
        assertEquals("0", rerankedHits.getAt(0).getId());
        assertEquals(0.9f, rerankedHits.getAt(0).getScore(), 0.0001f);
        assertEquals("1", rerankedHits.getAt(1).getId());
        assertEquals(0.7f, rerankedHits.getAt(1).getScore(), 0.0001f);
        assertEquals("2", rerankedHits.getAt(2).getId());
        assertTrue(Float.isNaN(rerankedHits.getAt(2).getScore()));

        SearchHits keywordChunks = rerankedHits.getAt(0).getInnerHits().get("keyword_chunks");
        assertEquals(1, keywordChunks.getAt(0).getNestedIdentity().getOffset());
        assertEquals(0.9f, keywordChunks.getAt(0).getScore(), 0.0001f);
        assertEquals(0, keywordChunks.getAt(1).getNestedIdentity().getOffset());
        assertEquals(0.1f, keywordChunks.getAt(1).getScore(), 0.0001f);
        assertEquals(0.9f, keywordChunks.getMaxScore(), 0.0001f);

        SearchHits semanticChunks = rerankedHits.getAt(0).getInnerHits().get("semantic_chunks");
        assertEquals(1, semanticChunks.getAt(0).getNestedIdentity().getOffset());
        assertEquals(0.9f, semanticChunks.getAt(0).getScore(), 0.0001f);
        assertEquals(2, semanticChunks.getAt(1).getNestedIdentity().getOffset());
        assertEquals(0.5f, semanticChunks.getAt(1).getScore(), 0.0001f);
    }

    @SneakyThrows
    public void testRerank_whenInnerHitsSortedByField_thenKeepsOrderAndUpdatesScores() {
        SearchHit newer = createChunk(0, "alpha");
        newer.sortValues(new Object[] { 2L }, new DocValueFormat[] { DocValueFormat.RAW });
        SearchHit older = createChunk(1, "beta");
        older.sortValues(new Object[] { 1L }, new DocValueFormat[] { DocValueFormat.RAW });
        SearchHit parent = createParentHit(0, "0");
        parent.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(newer, older))));
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.1f, 0.9f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));
        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHit rerankedParent = argCaptor.getValue().getHits().getAt(0);
        assertEquals(0.9f, rerankedParent.getScore(), 0.0001f);
        SearchHits chunks = rerankedParent.getInnerHits().get("chunks");
        assertEquals(0, chunks.getAt(0).getNestedIdentity().getOffset());
        assertEquals(0.1f, chunks.getAt(0).getScore(), 0.0001f);
        assertEquals(1, chunks.getAt(1).getNestedIdentity().getOffset());
        assertEquals(0.9f, chunks.getAt(1).getScore(), 0.0001f);
        assertEquals(0.9f, chunks.getMaxScore(), 0.0001f);
    }

    @SneakyThrows
    public void testRerank_whenModelReturnsWrongNumberOfChunkScores_thenFailsWithoutTouchingHits() {
        SearchHit chunk0 = createChunk(0, "alpha");
        SearchHit chunk1 = createChunk(1, "beta");
        SearchHit parent = createParentHit(0, "0");
        parent.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(chunk0, chunk1))));
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.1f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));
        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assertEquals("Expected 2 chunk scores from the rerank model, got 1", argCaptor.getValue().getMessage());
        assertEquals(1.0f, parent.getScore(), 0.0f);
        assertSame(chunk0, parent.getInnerHits().get("chunks").getAt(0));
        assertTrue(Float.isNaN(chunk0.getScore()));
    }

    public void testRerank_whenNoChunksOnPage_thenKeepsOriginalScoresWithoutCallingModel() {
        SearchHit parent0 = createParentHit(0, "0");
        parent0.score(2.0f);
        SearchHit parent1 = createParentHit(1, "1");
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent0, parent1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 2.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        Map<String, Object> scoringContext = Map.of(
            QueryContextSourceFetcher.QUERY_TEXT_FIELD,
            "query text",
            DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD,
            List.of(),
            InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD,
            List.of(List.of(), List.of())
        );

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        verify(mlCommonsClientAccessor, never()).inferenceSimilarity(any(), any());
        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHits rerankedHits = argCaptor.getValue().getHits();
        assertEquals("0", rerankedHits.getAt(0).getId());
        assertEquals(2.0f, rerankedHits.getAt(0).getScore(), 0.0f);
        assertEquals("1", rerankedHits.getAt(1).getId());
        assertEquals(1.0f, rerankedHits.getAt(1).getScore(), 0.0f);
        assertEquals(2.0f, rerankedHits.getMaxScore(), 0.0f);
    }

    @SneakyThrows
    public void testRerank_whenChunksMissField_thenTheyAreUnscoredAndRankLast() {
        SearchHit parent0 = createParentHit(0, "0");
        parent0.setInnerHits(
            new LinkedHashMap<>(
                Map.of("chunks", innerHitsList(createChunk(0, "alpha"), createChunkWithoutText(1), createChunk(2, "gamma")))
            )
        );
        SearchHit parent1 = createParentHit(1, "1");
        parent1.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(createChunkWithoutText(0)))));
        SearchHit parent2 = createParentHit(2, "2");
        parent2.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(createChunk(0, "delta")))));
        TotalHits totalHits = new TotalHits(3, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent0, parent1, parent2 }, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.2f, 0.6f, 0.9f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));
        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<SimilarityInferenceRequest> requestCaptor = ArgumentCaptor.forClass(SimilarityInferenceRequest.class);
        verify(mlCommonsClientAccessor, times(1)).inferenceSimilarity(requestCaptor.capture(), isA(ActionListener.class));
        assertEquals(List.of("alpha", "gamma", "delta"), requestCaptor.getValue().getInputTexts());

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHits rerankedHits = argCaptor.getValue().getHits();
        assertEquals("2", rerankedHits.getAt(0).getId());
        assertEquals(0.9f, rerankedHits.getAt(0).getScore(), 0.0001f);
        assertEquals("0", rerankedHits.getAt(1).getId());
        assertEquals(0.6f, rerankedHits.getAt(1).getScore(), 0.0001f);
        assertEquals("1", rerankedHits.getAt(2).getId());
        assertTrue(Float.isNaN(rerankedHits.getAt(2).getScore()));

        SearchHits chunks = rerankedHits.getAt(1).getInnerHits().get("chunks");
        assertEquals(2, chunks.getAt(0).getNestedIdentity().getOffset());
        assertEquals(0, chunks.getAt(1).getNestedIdentity().getOffset());
        assertEquals(1, chunks.getAt(2).getNestedIdentity().getOffset());
        assertTrue(Float.isNaN(chunks.getAt(2).getScore()));
        assertEquals(0.6f, chunks.getMaxScore(), 0.0001f);
        assertTrue(Float.isNaN(rerankedHits.getAt(2).getInnerHits().get("chunks").getMaxScore()));
    }

    public void testRerank_whenNoChunkHasField_thenAllUnscoredWithoutCallingModel() {
        SearchHit parent0 = createParentHit(0, "0");
        parent0.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(createChunkWithoutText(0)))));
        SearchHit parent1 = createParentHit(1, "1");
        parent1.setInnerHits(new LinkedHashMap<>(Map.of("chunks", innerHitsList(createChunkWithoutText(0)))));
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent0, parent1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        verify(mlCommonsClientAccessor, never()).inferenceSimilarity(any(), any());
        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHits rerankedHits = argCaptor.getValue().getHits();
        assertTrue(Float.isNaN(rerankedHits.getAt(0).getScore()));
        assertTrue(Float.isNaN(rerankedHits.getAt(1).getScore()));
        assertTrue(Float.isNaN(rerankedHits.getAt(0).getInnerHits().get("chunks").getAt(0).getScore()));
    }

    @SneakyThrows
    public void testRerank_whenModelReturnsWrongNumberOfScoresWithMissingTexts_thenFails() {
        SearchHit parent = createParentHit(0, "0");
        parent.setInnerHits(
            new LinkedHashMap<>(
                Map.of("chunks", innerHitsList(createChunk(0, "alpha"), createChunkWithoutText(1), createChunk(2, "gamma")))
            )
        );
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);
        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.1f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));
        Map<String, Object> scoringContext = new HashMap<>(innerHitsContext(response));
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<Exception> argCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argCaptor.capture());
        assertEquals("Expected 2 scores from the rerank model, got 1", argCaptor.getValue().getMessage());
        assertEquals(1.0f, parent.getScore(), 0.0f);
    }

    public void testRerank_whenPageIsPastTheEnd_thenReturnsResponseUnchanged() {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(5, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD, List.of()), listener);

        verify(listener, times(1)).onResponse(response);
        verify(mlCommonsClientAccessor, never()).inferenceSimilarity(any(), any());
    }

    @SneakyThrows
    public void testRerank_whenChunksAreInnerHitsOfInnerHits_thenScoresAndSortsThem() {
        SearchHit section = new SearchHit(0, "section-0", new SearchHit.NestedIdentity("sections", 0, null), Map.of(), Map.of());
        section.setInnerHits(Map.of("sections.chunks", innerHitsList(createSectionChunk(0, "alpha"), createSectionChunk(1, "beta"))));
        // A section without chunks and an empty list must be skipped
        SearchHit sectionWithoutChunks = new SearchHit(
            1,
            "section-1",
            new SearchHit.NestedIdentity("sections", 1, null),
            Map.of(),
            Map.of()
        );
        SearchHit parent = createParentHit(0, "0");
        parent.setInnerHits(Map.of("sections", innerHitsList(section, sectionWithoutChunks), "other", innerHitsList()));
        SearchHits searchHits = new SearchHits(new SearchHit[] { parent }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new Clusters(1, 1, 0), null);

        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(0.2f, 0.8f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));

        Map<String, Object> scoringContext = new HashMap<>(
            innerHitsContext(new InnerHitsContextSourceFetcher("sections.chunks", "text"), response)
        );
        scoringContext.put(QueryContextSourceFetcher.QUERY_TEXT_FIELD, "query text");

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, scoringContext, listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchHit reranked = argCaptor.getValue().getHits().getAt(0);
        assertEquals(0.8f, reranked.getScore(), 0.0001f);
        SearchHits chunks = reranked.getInnerHits().get("sections").getAt(0).getInnerHits().get("sections.chunks");
        assertEquals(1, chunks.getAt(0).getNestedIdentity().getChild().getOffset());
        assertEquals(0.8f, chunks.getAt(0).getScore(), 0.0001f);
        assertEquals(0, chunks.getAt(1).getNestedIdentity().getChild().getOffset());
        assertEquals(0.2f, chunks.getAt(1).getScore(), 0.0001f);
        assertEquals(0.8f, chunks.getMaxScore(), 0.0001f);
        assertNull(reranked.getInnerHits().get("sections").getAt(1).getInnerHits());
        assertEquals(0, reranked.getInnerHits().get("other").getHits().length);
    }

    private SearchHit createSectionChunk(int offset, String text) {
        SearchHit.NestedIdentity identity = new SearchHit.NestedIdentity(
            "sections",
            0,
            new SearchHit.NestedIdentity("chunks", offset, null)
        );
        SearchHit chunk = new SearchHit(offset, "chunk-" + offset, identity, Map.of(), Map.of());
        chunk.sourceRef(new BytesArray(String.format(Locale.ROOT, "{\"text\": \"%s\"}", text)));
        return chunk;
    }

    private Map<String, Object> innerHitsContext(SearchResponse searchResponse) {
        return innerHitsContext(new InnerHitsContextSourceFetcher("embeddings", "text"), searchResponse);
    }

    @SneakyThrows
    private Map<String, Object> innerHitsContext(InnerHitsContextSourceFetcher fetcher, SearchResponse searchResponse) {
        List<Map<String, Object>> captured = new ArrayList<>();
        fetcher.fetchContext(request, searchResponse, ActionListener.wrap(captured::add, e -> fail("Should not fail: " + e)));
        return captured.get(0);
    }

    private SearchHit createParentHit(int docId, String id) {
        SearchHit hit = new SearchHit(docId, id, Map.of(), Map.of());
        hit.score(1.0f);
        return hit;
    }

    private SearchHit createChunk(int offset, String text) {
        SearchHit chunk = new SearchHit(
            offset,
            "chunk-" + offset,
            new SearchHit.NestedIdentity("embeddings", offset, null),
            Map.of(),
            Map.of()
        );
        chunk.sourceRef(new BytesArray(String.format(Locale.ROOT, "{\"text\": \"%s\"}", text)));
        return chunk;
    }

    private SearchHit createChunkWithoutText(int offset) {
        SearchHit chunk = new SearchHit(
            offset,
            "chunk-" + offset,
            new SearchHit.NestedIdentity("embeddings", offset, null),
            Map.of(),
            Map.of()
        );
        chunk.sourceRef(new BytesArray("{}"));
        return chunk;
    }

    private SearchHits innerHitsList(SearchHit... hits) {
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
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
        doAnswer(invocation -> {
            ActionListener<List<Float>> inferenceListener = invocation.getArgument(1);
            inferenceListener.onResponse(List.of(1f, 2f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSimilarity(isA(SimilarityInferenceRequest.class), isA(ActionListener.class));
        setupSearchResults();
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.processResponseAsync(request, response, ppctx, listener);
        // The hit without the context field is not sent to the model and ranks last with a NaN score
        ArgumentCaptor<SimilarityInferenceRequest> requestCaptor = ArgumentCaptor.forClass(SimilarityInferenceRequest.class);
        verify(mlCommonsClientAccessor, times(1)).inferenceSimilarity(requestCaptor.capture(), isA(ActionListener.class));
        assertEquals(List.of("field passage", "source passage"), requestCaptor.getValue().getInputTexts());
        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse rsp = argCaptor.getValue();
        assertEquals(0, rsp.getHits().getAt(0).docId());
        assertEquals(2f, rsp.getHits().getAt(0).getScore(), 0.0f);
        assertEquals(1, rsp.getHits().getAt(1).docId());
        assertEquals(1f, rsp.getHits().getAt(1).getScore(), 0.0f);
        assertEquals(2, rsp.getHits().getAt(2).docId());
        assertTrue(Float.isNaN(rsp.getHits().getAt(2).getScore()));
    }
}
