/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.common.VectorUtil;
import org.opensearch.neuralsearch.constants.SemanticFieldConstants;
import org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.mapper.dto.ChunkingConfig;
import org.opensearch.neuralsearch.mapper.dto.SemanticParameters;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE;
import static org.opensearch.neuralsearch.query.NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_TWO;
import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

public class NeuralQueryBuilderRewriteTests extends OpenSearchTestCase {
    private static final String FIELD_NAME = "testField";
    private static final String CUSTOM_SEMANTIC_INFO_FIELD_NAME = "custom_semantic_info_field";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String IMAGE_TEXT = "base641234567890";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final Integer K = 10;
    private static final String SEARCH_ANALYZER = "standard";

    private static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[10];

    private static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();

    private static final String REMOTE_INDEX_NAME = "remote:nlp-index";
    private static final String LOCAL_INDEX_NAME = "nlp-index";
    private static final String LOCAL_INDEX_NAME_2 = "nlp-index-2";
    private static final String MODEL_ID_1 = "modelId1";
    private static final String MODEL_ID_2 = "modelId2";
    private static final List<Number> TEST_VECTOR_LIST = List.of(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
    private static final List<Number> TEST_VECTOR_LIST_2 = List.of(5.0f, 4.0f, 3.0f, 2.0f, 1.0f);
    private static final List<Map<String, ?>> TEST_QUERY_TOKENS = List.of(Map.of("response", List.of(Map.of("key1", 1.0f, "key2", 2.0f))));
    private static final List<Map<String, ?>> TEST_QUERY_TOKENS_2 = List.of(
        Map.of("response", List.of(Map.of("key1", 2.0f, "key2", 1.0f)))
    );
    private static final List<Map<String, ?>> TEST_QUERY_TOKENS_3 = List.of(
        Map.of("response", List.of(Map.of("high_score_token", 0.6f, "low_score_token", 0.1f)))
    );

    private static final Float DELTA = 1e-4f;
    private static final float PRUNE_RATIO = 0.4f;
    private static final PruneType PRUNE_TYPE = PruneType.MAX_RATIO;

    private MLCommonsClientAccessor mlClient;

    @Before
    public void setup() throws Exception {
        mlClient = mock(MLCommonsClientAccessor.class);
        NeuralQueryBuilder.initialize(mlClient);
        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();
    }

    @SneakyThrows
    public void testRewrite_whenVectorSupplierNull_v3_0_0_thenSetVectorSupplier() {
        setUpClusterService(Version.V_3_0_0);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();
        List<Number> expectedVector = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Number>> listener = invocation.getArgument(1);
            listener.onResponse(expectedVector);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));
        NeuralQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set vector supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralQueryBuilder queryBuilder = (NeuralQueryBuilder) neuralQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.vectorSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertArrayEquals(VectorUtil.vectorAsListToArray(expectedVector), queryBuilder.vectorSupplier().get(), 0.0f);
    }

    @SneakyThrows
    public void testRewrite_whenVectorSupplierNullAndQueryTextAndImageTextSet_v3_0_0_thenSetVectorSupplier() {
        setUpClusterService(Version.V_3_0_0);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();
        List<Number> expectedVector = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f, 5.0f);
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        doAnswer(invocation -> {
            ActionListener<List<Number>> listener = invocation.getArgument(1);
            listener.onResponse(expectedVector);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));
        NeuralQueryBuilder.initialize(mlCommonsClientAccessor);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set vector supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NeuralQueryBuilder queryBuilder = (NeuralQueryBuilder) neuralQueryBuilder.doRewrite(queryRewriteContext);
        assertNotNull(queryBuilder.vectorSupplier());
        assertTrue(inProgressLatch.await(5, TimeUnit.SECONDS));
        assertArrayEquals(VectorUtil.vectorAsListToArray(expectedVector), queryBuilder.vectorSupplier().get(), 0.0f);
    }

    public void testRewrite_whenVectorNull_v3_0_0_thenReturnCopy() {
        setUpClusterService(Version.V_3_0_0);
        Supplier<float[]> nullSupplier = () -> null;
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .expandNested(Boolean.TRUE)
            .vectorSupplier(nullSupplier)
            .build();
        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertEquals(neuralQueryBuilder, queryBuilder);
    }

    public void testRewrite_whenVectorSupplierAndVectorSet_v3_0_0_thenReturnKNNQueryBuilder() {
        setUpClusterService(Version.V_3_0_0);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .expandNested(Boolean.TRUE)
            .methodParameters(Map.of("ef_search", 100))
            .rescoreContext(RescoreContext.getDefault())
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .build();

        NeuralKNNQueryBuilder expected = NeuralKNNQueryBuilder.builder()
            .k(K)
            .fieldName(neuralQueryBuilder.fieldName())
            .methodParameters(neuralQueryBuilder.methodParameters())
            .rescoreContext(neuralQueryBuilder.rescoreContext())
            .vector(TEST_VECTOR_SUPPLIER.get())
            .expandNested(Boolean.TRUE)
            .originalQueryText(QUERY_TEXT)
            .build();

        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertEquals(expected, queryBuilder);
    }

    public void testRewrite_whenFilterSet_v3_0_0_thenKNNQueryBuilderFilterSet() {
        setUpClusterService(Version.V_3_0_0);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .filter(TEST_FILTER)
            .build();
        QueryBuilder queryBuilder = neuralQueryBuilder.doRewrite(null);
        assertTrue(queryBuilder instanceof NeuralKNNQueryBuilder);
        NeuralKNNQueryBuilder neuralKNNQueryBuilder = (NeuralKNNQueryBuilder) queryBuilder;
        assertEquals(neuralQueryBuilder.queryfilter(), neuralKNNQueryBuilder.getKnnQueryBuilder().getFilter());
    }

    public void testRewrite_whenTargetKnnInRemoteCluster_3_0_0_thenKnnQueryBuilder() {
        setUpClusterService(Version.V_3_0_0);

        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);

        // test
        testRewriteTargetKnn(queryCoordinatorContext);
    }

    public void testRewrite_whenTargetKnnInRemoteCluster_thenKnnQueryBuilder() {
        setUpClusterService();

        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(REMOTE_INDEX_NAME).toArray(new String[0]));

        // test
        testRewriteTargetKnn(queryCoordinatorContext);
    }

    public void testRewrite_whenTargetKnn_thenKnnQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(LOCAL_INDEX_NAME, Map.of(PROPERTIES, Map.of(FIELD_NAME, Map.of(TYPE, KNNVectorFieldMapper.CONTENT_TYPE)))),
            indicesRequest
        );

        // test
        testRewriteTargetKnn(queryCoordinatorContext);
    }

    public void testRewrite_whenTargetKnnAndOnShardDirectly_thenKnnQueryBuilder() {
        setUpClusterService();
        // prepare data to rewrite on shard level
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        final KNNVectorFieldType knnVectorFieldType = mock(KNNVectorFieldType.class);
        when(queryShardContext.convertToShardContext()).thenReturn(queryShardContext);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(knnVectorFieldType);
        when(knnVectorFieldType.typeName()).thenReturn(KNNVectorFieldMapper.CONTENT_TYPE);

        // test
        testRewriteTargetKnn(queryShardContext);
    }

    private void testRewriteTargetKnn(QueryRewriteContext queryRewriteContext) {
        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryRewriteContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryRewriteContext);

        // verify
        // first rewrite we start an async action to generate the embedding and return a new neural query instance with
        // the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).vectorSupplier());
        assertNull(((NeuralQueryBuilder) rewritten1).vectorSupplier().get());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Number>> listener = (ActionListener<List<Number>>) invocation.getArguments()[1];
            listener.onResponse(TEST_VECTOR_LIST);
            return null;
        }).when(mlClient).inferenceSentencesMap(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final float[] expectedVector = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        assertArrayEquals(expectedVector, ((NeuralQueryBuilder) rewritten1).vectorSupplier().get(), 1e-6f);

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryRewriteContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder
        assertTrue(rewritten2 instanceof NeuralKNNQueryBuilder);
    }

    public void testRewriteTargetSemanticKnn_thenNestedKnnQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).k(K).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start one async actions to generate the embedding. Then return a new neural query
        // instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Number>> listener = (ActionListener<List<Number>>) invocation.getArguments()[1];
            listener.onResponse(TEST_VECTOR_LIST);
            return null;
        }).when(mlClient).inferenceSentencesMap(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final float[] expectedVector1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        assertArrayEquals(expectedVector1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get(), 1e-6f);

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten2 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten2).query() instanceof NeuralKNNQueryBuilder);
        assertArrayEquals(
            expectedVector1,
            (float[]) ((NeuralKNNQueryBuilder) ((NestedQueryBuilder) rewritten2).query()).getKnnQueryBuilder().vector(),
            1e-6f
        );
    }

    public void testRewriteTargetSemanticKnn_whenSearchAnalyzerInQuery_thenException() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .k(K)
            .searchAnalyzer(SEARCH_ANALYZER)
            .build();

        // first rewrite on the coordinate level
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> neuralQueryBuilder.doRewrite(queryCoordinatorContext)
        );

        final String expectedError = "Invalid neural query: Target field is a semantic field using a dense model."
            + " semantic_field_search_analyzer is not supported since it is for the sparse model.";
        assertEquals(expectedError, exception.getMessage());

    }

    private List<BiConsumer<Client, ActionListener<Void>>> mockRegisterAsyncAction(QueryRewriteContext queryRewriteContext) {
        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = new ArrayList<>();
        doAnswer(invocation -> {
            asyncActions.add(invocation.getArgument(0));
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());
        return asyncActions;
    }

    public void testRewriteTargetSemanticKnn_whenMultipleTargetIndices_thenNestedKnnQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME, LOCAL_INDEX_NAME_2).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                ),
                LOCAL_INDEX_NAME_2,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_2)
                        .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(false)
                        .semanticInfoName(CUSTOM_SEMANTIC_INFO_FIELD_NAME)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).k(K).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start two async actions to generate the embedding since the two target indices are using
        // different model. Then return a new neural query instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_2).get());
        assertEquals(2, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().size());
        assertEquals(2, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final MapInferenceRequest inferenceRequest = (MapInferenceRequest) invocation.getArguments()[0];
            final ActionListener<List<Number>> listener = (ActionListener<List<Number>>) invocation.getArguments()[1];
            if (MODEL_ID_1.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_VECTOR_LIST);
            } else if (MODEL_ID_2.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_VECTOR_LIST_2);
            }
            return null;
        }).when(mlClient).inferenceSentencesMap(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));
        asyncActions.get(1).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final float[] expectedVector1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        final float[] expectedVector2 = { 5.0f, 4.0f, 3.0f, 2.0f, 1.0f };
        assertArrayEquals(expectedVector1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get(), 1e-6f);
        assertArrayEquals(expectedVector2, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_2).get(), 1e-6f);

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the query should be rewritten as NeuralQueryBuilder
        assertTrue(rewritten2 instanceof NeuralQueryBuilder);

        // prepare data for rewrite on the shard level for the first index LOCAL_INDEX_NAME
        final QueryShardContext queryShardContext = mockQueryShardContextForKnn(MODEL_ID_1, null, true);

        QueryBuilder rewritten3 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten3 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten3).query() instanceof NeuralKNNQueryBuilder);
        assertArrayEquals(
            expectedVector1,
            (float[]) ((NeuralKNNQueryBuilder) ((NestedQueryBuilder) rewritten3).query()).getKnnQueryBuilder().vector(),
            1e-6f
        );

        // prepare data for rewrite on the shard level for the second index LOCAL_INDEX_NAME_2
        final QueryShardContext queryShardContext2 = mockQueryShardContextForKnn(MODEL_ID_2, CUSTOM_SEMANTIC_INFO_FIELD_NAME, false);

        QueryBuilder rewritten4 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext2);

        // verify the query should be rewritten as NeuralKNNQueryBuilder since the chunking is disabled
        assertTrue(rewritten4 instanceof NeuralKNNQueryBuilder);
        assertArrayEquals(expectedVector2, (float[]) ((NeuralKNNQueryBuilder) rewritten4).getKnnQueryBuilder().vector(), 1e-6f);
    }

    public void testRewriteTargetSemanticKnn_whenModelIdInQuery_thenNestedKnnQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .modelId(MODEL_ID_2)
            .queryText(QUERY_TEXT)
            .k(K)
            .build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start one async actions to generate the embedding. Then return a new neural query
        // instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap());
        // We will use the model in the neural query to override the one defined in the semantic field
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1));
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_2).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Number>> listener = (ActionListener<List<Number>>) invocation.getArguments()[1];
            listener.onResponse(TEST_VECTOR_LIST);
            return null;
        }).when(mlClient).inferenceSentencesMap(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final float[] expectedVector1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        assertArrayEquals(expectedVector1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_2).get(), 1e-6f);

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten2 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten2).query() instanceof NeuralKNNQueryBuilder);
        assertArrayEquals(
            expectedVector1,
            (float[]) ((NeuralKNNQueryBuilder) ((NestedQueryBuilder) rewritten2).query()).getKnnQueryBuilder().vector(),
            1e-6f
        );
    }

    public void testRewriteTargetSemanticKnn_whenOnShardDirectly_thenNestedKnnQueryBuilder() {
        setUpClusterService();
        // prepare data to rewrite on the shard directly
        final QueryShardContext queryShardContext = mockQueryShardContextForKnn(MODEL_ID_1, null, true);

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryShardContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).k(K).build();

        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryShardContext);

        // verify
        // first rewrite we start one async actions to generate the embedding. Then return a new neural query
        // instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Number>> listener = (ActionListener<List<Number>>) invocation.getArguments()[1];
            listener.onResponse(TEST_VECTOR_LIST);
            return null;
        }).when(mlClient).inferenceSentencesMap(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final float[] expectedVector1 = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f };
        assertArrayEquals(expectedVector1, ((NeuralQueryBuilder) rewritten1).modelIdToVectorSupplierMap().get(MODEL_ID_1).get(), 1e-6f);

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryShardContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten2 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten2).query() instanceof NeuralKNNQueryBuilder);
        assertArrayEquals(
            expectedVector1,
            (float[]) ((NeuralKNNQueryBuilder) ((NestedQueryBuilder) rewritten2).query()).getKnnQueryBuilder().vector(),
            1e-6f
        );
    }

    private QueryShardContext mockQueryShardContextForKnn(
        final String modelId,
        final String semanticInfoFieldName,
        final Boolean chunkingEnabled
    ) {
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        final SemanticFieldMapper.SemanticFieldType semanticFieldType = mock(SemanticFieldMapper.SemanticFieldType.class);
        final KNNVectorFieldType knnVectorFieldType = mock(KNNVectorFieldType.class);
        when(queryShardContext.convertToShardContext()).thenReturn(queryShardContext);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(semanticFieldType);
        final String semanticInfoFieldPath = semanticInfoFieldName == null ? FIELD_NAME + "_semantic_info" : semanticInfoFieldName;
        String embeddingFullPath;
        final SemanticParameters.SemanticParametersBuilder semanticParametersBuilder = SemanticParameters.builder()
            .modelId(modelId)
            .rawFieldType(TextFieldMapper.CONTENT_TYPE)
            .semanticInfoFieldName(semanticInfoFieldName);
        if (Boolean.TRUE.equals(chunkingEnabled)) {
            embeddingFullPath = semanticInfoFieldPath + ".chunks.embedding";
            semanticParametersBuilder.chunkingConfig(ChunkingConfig.builder().enabled(true).build());
        } else {
            embeddingFullPath = semanticInfoFieldPath + ".embedding";
            semanticParametersBuilder.chunkingConfig(null);
        }

        when(queryShardContext.fieldMapper(embeddingFullPath)).thenReturn(knnVectorFieldType);
        when(semanticFieldType.getSemanticParameters()).thenReturn(semanticParametersBuilder.build());
        when(semanticFieldType.name()).thenReturn(FIELD_NAME);
        when(semanticFieldType.typeName()).thenReturn(SemanticFieldMapper.CONTENT_TYPE);
        when(semanticFieldType.getSemanticInfoFieldPath()).thenReturn(semanticInfoFieldPath);
        when(knnVectorFieldType.typeName()).thenReturn(KNNVectorFieldMapper.CONTENT_TYPE);
        return queryShardContext;
    }

    public void testRewriteTargetSemanticRankFeatures_thenNestedNeuralSparseQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start one async actions to generate the embedding. Then return a new neural query
        // instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Map<String, ?>>> listener = (ActionListener<List<Map<String, ?>>>) invocation.getArguments()[1];
            listener.onResponse(TEST_QUERY_TOKENS);
            return null;
        }).when(mlClient).inferenceSentencesWithMapResult(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final Map<String, Float> expectedTokenMap = Map.of("key1", 1.0f, "key2", 2.0f);
        assertEquals(expectedTokenMap, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the query should be rewritten as NeuralKNNQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten2 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten2).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(
            expectedTokenMap,
            ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten2).query()).queryTokensMapSupplier().get()
        );
    }

    public void testRewriteTargetSemanticRankFeatures_whenMultipleTargetIndices_thenSuccess() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME, LOCAL_INDEX_NAME_2).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(false)
                        .build()
                ),
                LOCAL_INDEX_NAME_2,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_2)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .semanticInfoName(CUSTOM_SEMANTIC_INFO_FIELD_NAME)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start two async actions to generate the embedding since the two target indices are using
        // different model. Then return a new neural query instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_2).get());
        assertEquals(2, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().size());
        assertEquals(2, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final TextInferenceRequest inferenceRequest = (TextInferenceRequest) invocation.getArguments()[0];
            final ActionListener<List<Map<String, ?>>> listener = (ActionListener<List<Map<String, ?>>>) invocation.getArguments()[1];
            if (MODEL_ID_1.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_QUERY_TOKENS);
            } else if (MODEL_ID_2.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_QUERY_TOKENS_2);
            }
            return null;
        }).when(mlClient).inferenceSentencesWithMapResult(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));
        asyncActions.get(1).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final Map<String, Float> expectedTokens1 = Map.of("key1", 1.0f, "key2", 2.0f);
        final Map<String, Float> expectedTokens2 = Map.of("key1", 2.0f, "key2", 1.0f);
        assertEquals(expectedTokens1, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());
        assertEquals(expectedTokens2, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_2).get());

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the query should be rewritten as NeuralQueryBuilder
        assertTrue(rewritten2 instanceof NeuralQueryBuilder);

        // prepare data for rewrite on the shard level for the first index LOCAL_INDEX_NAME
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        final Index index = mock(Index.class);
        when(queryShardContext.index()).thenReturn(index);
        when(queryShardContext.convertToShardContext()).thenReturn(queryShardContext);
        when(index.getName()).thenReturn(LOCAL_INDEX_NAME);
        final SemanticFieldMapper.SemanticFieldType semanticFieldType = mock(SemanticFieldMapper.SemanticFieldType.class);
        final RankFeaturesFieldMapper.RankFeaturesFieldType rankFeaturesFieldType = new RankFeaturesFieldMapper.RankFeaturesFieldType(
            FIELD_NAME,
            new HashMap<>(),
            true
        );
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(semanticFieldType);
        when(queryShardContext.fieldMapper(FIELD_NAME + "_semantic_info.embedding")).thenReturn(rankFeaturesFieldType);

        when(semanticFieldType.getSemanticParameters()).thenReturn(
            SemanticParameters.builder().modelId(MODEL_ID_1).rawFieldType(TextFieldMapper.CONTENT_TYPE).build()
        );
        when(semanticFieldType.name()).thenReturn(FIELD_NAME);
        when(semanticFieldType.typeName()).thenReturn(SemanticFieldMapper.CONTENT_TYPE);
        when(semanticFieldType.getSemanticInfoFieldPath()).thenReturn(FIELD_NAME + "_semantic_info");

        QueryBuilder rewritten3 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext);

        // verify the query should be rewritten as NeuralSparseQueryBuilder since the chunking is disabled
        assertTrue(rewritten3 instanceof NeuralSparseQueryBuilder);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = (NeuralSparseQueryBuilder) rewritten3;
        assertEquals(expectedTokens1, neuralSparseQueryBuilder.queryTokensMapSupplier().get());

        // prepare data for rewrite on the shard level for the second index LOCAL_INDEX_NAME_2
        final QueryShardContext queryShardContext2 = mock(QueryShardContext.class);
        final Index index2 = mock(Index.class);
        when(queryShardContext2.index()).thenReturn(index2);
        when(queryShardContext2.convertToShardContext()).thenReturn(queryShardContext2);
        when(index2.getName()).thenReturn(LOCAL_INDEX_NAME_2);
        final SemanticFieldMapper.SemanticFieldType semanticFieldType2 = mock(SemanticFieldMapper.SemanticFieldType.class);
        when(queryShardContext2.fieldMapper(FIELD_NAME)).thenReturn(semanticFieldType2);
        when(queryShardContext2.fieldMapper(CUSTOM_SEMANTIC_INFO_FIELD_NAME + ".chunks.embedding")).thenReturn(rankFeaturesFieldType);
        when(semanticFieldType2.getSemanticParameters()).thenReturn(
            SemanticParameters.builder()
                .modelId(MODEL_ID_2)
                .rawFieldType(TextFieldMapper.CONTENT_TYPE)
                .semanticInfoFieldName(CUSTOM_SEMANTIC_INFO_FIELD_NAME)
                .chunkingConfig(ChunkingConfig.builder().enabled(true).build())
                .build()
        );
        when(semanticFieldType2.name()).thenReturn(FIELD_NAME);
        when(semanticFieldType2.typeName()).thenReturn(SemanticFieldMapper.CONTENT_TYPE);
        when(semanticFieldType2.getSemanticInfoFieldPath()).thenReturn(CUSTOM_SEMANTIC_INFO_FIELD_NAME);

        QueryBuilder rewritten4 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext2);

        // verify the query should be rewritten as NeuralSparseQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten4 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten4).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(
            expectedTokens2,
            ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten4).query()).queryTokensMapSupplier().get()
        );
    }

    public void testRewriteTargetSemanticRankFeatures_whenMultipleTargetIndicesOneWithAnalyzer_thenSuccess() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME, LOCAL_INDEX_NAME_2).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(false)
                        .searchAnalyzer(SEARCH_ANALYZER)
                        .build()
                ),
                LOCAL_INDEX_NAME_2,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_2)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .semanticInfoName(CUSTOM_SEMANTIC_INFO_FIELD_NAME)
                        .build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start one async action to generate the embedding since the one target index is using search
        // searchAnalyzer and another is using the model. Then return a new neural query instance with the same parameters
        // and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1));
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_2).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // mock async action is done
        doAnswer(invocation -> {
            final TextInferenceRequest inferenceRequest = (TextInferenceRequest) invocation.getArguments()[0];
            final ActionListener<List<Map<String, ?>>> listener = (ActionListener<List<Map<String, ?>>>) invocation.getArguments()[1];
            if (MODEL_ID_1.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_QUERY_TOKENS);
            } else if (MODEL_ID_2.equals(inferenceRequest.getModelId())) {
                listener.onResponse(TEST_QUERY_TOKENS_2);
            }
            return null;
        }).when(mlClient).inferenceSentencesWithMapResult(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the vector is set
        final Map<String, Float> expectedTokens2 = Map.of("key1", 2.0f, "key2", 1.0f);
        assertEquals(expectedTokens2, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_2).get());

        // second rewrite on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify no rewrite
        assertEquals(rewritten1, rewritten2);

        // prepare data for rewrite on the shard level for the first index LOCAL_INDEX_NAME
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        final Index index = mock(Index.class);
        when(queryShardContext.index()).thenReturn(index);
        when(queryShardContext.convertToShardContext()).thenReturn(queryShardContext);
        when(index.getName()).thenReturn(LOCAL_INDEX_NAME);
        final SemanticFieldMapper.SemanticFieldType semanticFieldType = mock(SemanticFieldMapper.SemanticFieldType.class);
        final RankFeaturesFieldMapper.RankFeaturesFieldType rankFeaturesFieldType = new RankFeaturesFieldMapper.RankFeaturesFieldType(
            FIELD_NAME,
            new HashMap<>(),
            true
        );
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(semanticFieldType);
        when(queryShardContext.fieldMapper(FIELD_NAME + "_semantic_info.embedding")).thenReturn(rankFeaturesFieldType);

        when(semanticFieldType.getSemanticParameters()).thenReturn(
            SemanticParameters.builder()
                .modelId(MODEL_ID_1)
                .rawFieldType(TextFieldMapper.CONTENT_TYPE)
                .semanticFieldSearchAnalyzer(SEARCH_ANALYZER)
                .build()
        );
        when(semanticFieldType.name()).thenReturn(FIELD_NAME);
        when(semanticFieldType.typeName()).thenReturn(SemanticFieldMapper.CONTENT_TYPE);
        when(semanticFieldType.getSemanticInfoFieldPath()).thenReturn(FIELD_NAME + "_semantic_info");

        QueryBuilder rewritten3 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext);

        // verify the query should be rewritten as NeuralSparseQueryBuilder since the chunking is disabled
        assertTrue(rewritten3 instanceof NeuralSparseQueryBuilder);
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = (NeuralSparseQueryBuilder) rewritten3;
        assertEquals(SEARCH_ANALYZER, neuralSparseQueryBuilder.searchAnalyzer());

        // prepare data for rewrite on the shard level for the second index LOCAL_INDEX_NAME_2
        final QueryShardContext queryShardContext2 = mock(QueryShardContext.class);
        final Index index2 = mock(Index.class);
        when(queryShardContext2.index()).thenReturn(index2);
        when(queryShardContext2.convertToShardContext()).thenReturn(queryShardContext2);
        when(index2.getName()).thenReturn(LOCAL_INDEX_NAME_2);
        final SemanticFieldMapper.SemanticFieldType semanticFieldType2 = mock(SemanticFieldMapper.SemanticFieldType.class);
        when(queryShardContext2.fieldMapper(FIELD_NAME)).thenReturn(semanticFieldType2);
        when(queryShardContext2.fieldMapper(CUSTOM_SEMANTIC_INFO_FIELD_NAME + ".chunks.embedding")).thenReturn(rankFeaturesFieldType);
        when(semanticFieldType2.getSemanticParameters()).thenReturn(
            SemanticParameters.builder()
                .modelId(MODEL_ID_2)
                .rawFieldType(TextFieldMapper.CONTENT_TYPE)
                .semanticInfoFieldName(CUSTOM_SEMANTIC_INFO_FIELD_NAME)
                .chunkingConfig(ChunkingConfig.builder().enabled(true).build())
                .build()
        );
        when(semanticFieldType2.name()).thenReturn(FIELD_NAME);
        when(semanticFieldType2.typeName()).thenReturn(SemanticFieldMapper.CONTENT_TYPE);
        when(semanticFieldType2.getSemanticInfoFieldPath()).thenReturn(CUSTOM_SEMANTIC_INFO_FIELD_NAME);

        QueryBuilder rewritten4 = ((NeuralQueryBuilder) rewritten2).doRewrite(queryShardContext2);

        // verify the query should be rewritten as NeuralSparseQueryBuilder in a NestedQueryBuilder
        assertTrue(rewritten4 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten4).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(
            expectedTokens2,
            ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten4).query()).queryTokensMapSupplier().get()
        );
    }

    public void testRewriteTargetSemanticRankFeatures_whenWithRawTokens_thenNestedNeuralSparseQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryTokensMapSupplier(() -> Map.of("key1", 1.0f))
            .build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite will directly rewrite it as NeuralKNNQueryBuilder in a NestedQueryBuilder since we have query
        // tokens and only target one index
        assertTrue(rewritten1 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten1).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(
            Map.of("key1", 1.0f),
            ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten1).query()).queryTokensMapSupplier().get()
        );
    }

    public void testRewriteTargetSemanticRankFeatures_whenSearchAnalyzerInQuery_thenNestedNeuralSparseQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .searchAnalyzer(SEARCH_ANALYZER)
            .build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite will directly rewrite it as NeuralKNNQueryBuilder in a NestedQueryBuilder since we have query
        // tokens and only target one index
        assertTrue(rewritten1 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten1).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(SEARCH_ANALYZER, ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten1).query()).searchAnalyzer());
    }

    public void testRewriteTargetSemanticRankFeatures_whenSearchAnalyzerInMapping_thenNestedNeuralSparseQueryBuilder() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .searchAnalyzer(SEARCH_ANALYZER)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite will directly rewrite it as NeuralKNNQueryBuilder in a NestedQueryBuilder since we have query
        // tokens and only target one index
        assertTrue(rewritten1 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten1).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(SEARCH_ANALYZER, ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten1).query()).searchAnalyzer());
    }

    public void testRewriteTargetSemanticRankFeatures_whenBothModelIdAndSearchAnalyzerAreProvidedInQuery_thenException() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .searchAnalyzer(SEARCH_ANALYZER)
            .build();

        // first rewrite on the coordinate level
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> neuralQueryBuilder.doRewrite(queryCoordinatorContext)
        );

        final String expectedError = "Invalid neural query: query_tokens, model_id and semantic_field_search_analyzer can not coexist";
        assertEquals(expectedError, exception.getMessage());
    }

    public
        void
        testRewriteTargetSemanticRankFeatures_whenBothModelIdAndSearchAnalyzerAreProvidedAcrossQueryAndMapping_thenUseTokenizerInQuery() {
        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .chunkingEnabled(true)
                        .searchModelId(MODEL_ID_2)
                        .build()
                )
            ),
            indicesRequest
        );

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .searchAnalyzer(SEARCH_ANALYZER)
            .build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten1 = neuralQueryBuilder.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite will directly rewrite it as NeuralKNNQueryBuilder in a NestedQueryBuilder since we have query
        // tokens and only target one index
        assertTrue(rewritten1 instanceof NestedQueryBuilder);
        assertTrue(((NestedQueryBuilder) rewritten1).query() instanceof NeuralSparseQueryBuilder);
        assertEquals(SEARCH_ANALYZER, ((NeuralSparseQueryBuilder) ((NestedQueryBuilder) rewritten1).query()).searchAnalyzer());
    }

    private void mockIndexMapping(final Map<String, Map<String, Object>> indexToMappingMap, final IndicesRequest indicesRequest) {
        final ClusterService clusterService = NeuralSearchClusterTestUtils.mockClusterService(Version.CURRENT);
        final IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
        final Metadata metadata = mock(Metadata.class);
        final Set<String> indices = indexToMappingMap.keySet();
        Map<String, Index> indexNameToIndexMap = new HashMap<>();
        for (String indexName : indices) {
            indexNameToIndexMap.put(indexName, new Index(indexName, "uuid"));
        }
        when(indexNameExpressionResolver.concreteIndices(clusterService.state(), indicesRequest)).thenReturn(
            indexNameToIndexMap.values().toArray(new Index[0])
        );
        when(clusterService.state().metadata()).thenReturn(metadata);

        for (Map.Entry<String, Map<String, Object>> entry : indexToMappingMap.entrySet()) {
            final Index index = indexNameToIndexMap.get(entry.getKey());
            final IndexMetadata indexMetadata = mock(IndexMetadata.class);
            final MappingMetadata mappingMetadata = mock(MappingMetadata.class);
            when(metadata.index(index)).thenReturn(indexMetadata);
            when(indexMetadata.mapping()).thenReturn(mappingMetadata);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(mappingMetadata.sourceAsMap()).thenReturn(entry.getValue());
        }

    }

    public void testRewriteTargetUnmappedField_thenNeuralQueryBuilder() {
        setUpClusterService();
        // prepare data to rewrite on coordinate level
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.fieldMapper(FIELD_NAME)).thenReturn(null);

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).k(K).build();

        // first rewrite on the coordinate level
        QueryBuilder rewritten = neuralQueryBuilder.doRewrite(queryShardContext);

        // verify the rewritten equals to the original query builder
        assertEquals(rewritten, neuralQueryBuilder);
    }

    public void testIsTargetSparseEmbedding_whenRemoteIndices_returnsFalse() {
        setUpClusterService();
        final IndicesRequest searchRequest = Mockito.mock(IndicesRequest.class);
        final NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        when(searchRequest.indices()).thenReturn(new String[] { "remote:index" });

        assertFalse(neuralQueryBuilder.isTargetSparseEmbedding(searchRequest));
    }

    public void testIsTargetSparseEmbedding_whenNotSparseEmbedding_returnsFalse() {
        setUpClusterService();
        final IndicesRequest searchRequest = mock(IndicesRequest.class);
        when(searchRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder().modelId(MODEL_ID_1).embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE).build()
                )
            ),
            searchRequest
        );

        final NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        assertFalse(neuralQueryBuilder.isTargetSparseEmbedding(searchRequest));
    }

    public void testIsTargetSparseEmbedding_whenSparseEmbedding_returnsTrue() {
        setUpClusterService();
        final IndicesRequest searchRequest = mock(IndicesRequest.class);
        when(searchRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder().modelId(MODEL_ID_1).embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE).build()
                )
            ),
            searchRequest
        );

        final NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryText(QUERY_TEXT).build();

        assertTrue(neuralQueryBuilder.isTargetSparseEmbedding(searchRequest));
    }

    public void testRewriteSparseTwoPhase_whenUseModel() {
        setUpClusterService();
        final NeuralQueryBuilder original = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .neuralSparseQueryTwoPhaseInfo(new NeuralSparseQueryTwoPhaseInfo(PHASE_ONE, PRUNE_RATIO, PRUNE_TYPE))
            .build();
        final NeuralQueryBuilder copy = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .neuralSparseQueryTwoPhaseInfo(new NeuralSparseQueryTwoPhaseInfo(PHASE_TWO, PRUNE_RATIO, PRUNE_TYPE))
            .modelIdToTwoPhaseSharedQueryTokenSupplier(original::modelIdToTwoPhaseSharedQueryToken)
            .build();

        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder().modelId(MODEL_ID_1).embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE).build()
                )
            ),
            indicesRequest
        );

        final List<BiConsumer<Client, ActionListener<Void>>> asyncActions = mockRegisterAsyncAction(queryCoordinatorContext);

        // first rewrite the original query on the coordinate level
        final QueryBuilder rewritten1 = original.doRewrite(queryCoordinatorContext);

        // verify
        // first rewrite we start one async actions to generate the embedding. Then return a new neural query
        // instance with the same parameters and vector supplier is set.
        assertTrue(rewritten1 instanceof NeuralQueryBuilder);
        assertNotNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap());
        assertNull(((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());
        assertEquals(1, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().size());
        assertEquals(1, asyncActions.size());

        // rewrite the copy as part of the rescorer of the query
        final QueryBuilder copyRewritten1 = copy.doRewrite(queryCoordinatorContext);

        // verify
        // directly rewrite it as NeuralSparseQueryBuilder since it's phase 2, and we target one index
        assertTrue(copyRewritten1 instanceof NeuralSparseQueryBuilder);
        // no inference call initiated for the copy since it will rely on the original query to share the tokens
        assertEquals(1, asyncActions.size());
        // for the query tokens are still null since inference call of the original query is not done yet
        assertNull(((NeuralSparseQueryBuilder) copyRewritten1).queryTokensMapSupplier.get());

        // mock async action is done
        doAnswer(invocation -> {
            final ActionListener<List<Map<String, ?>>> listener = (ActionListener<List<Map<String, ?>>>) invocation.getArguments()[1];
            listener.onResponse(TEST_QUERY_TOKENS_3);
            return null;
        }).when(mlClient).inferenceSentencesWithMapResult(any(), any());

        asyncActions.get(0).accept(mock(Client.class), mock(ActionListener.class));

        // verify the higher score tokens is set for the original query
        final Map<String, Float> highScoreToken = Map.of("high_score_token", 0.6f);
        assertEquals(highScoreToken, ((NeuralQueryBuilder) rewritten1).modelIdToQueryTokensSupplierMap().get(MODEL_ID_1).get());
        // verify the copy also have the lower score tokens set
        final Map<String, Float> lowScoreToken = Map.of("low_score_token", 0.1f);
        assertEquals(lowScoreToken, ((NeuralSparseQueryBuilder) copyRewritten1).queryTokensMapSupplier.get());

        // second rewrite the original query on the coordinate level
        QueryBuilder rewritten2 = ((NeuralQueryBuilder) rewritten1).doRewrite(queryCoordinatorContext);

        // verify the original query should be rewritten as NeuralSparseQueryBuilder
        assertTrue(rewritten2 instanceof NeuralSparseQueryBuilder);
        assertEquals(highScoreToken, ((NeuralSparseQueryBuilder) rewritten2).queryTokensMapSupplier().get());
    }

    public void testRewriteSparseTwoPhase_whenUseSearchAnalyzer() {
        setUpClusterService();
        final NeuralQueryBuilder original = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .neuralSparseQueryTwoPhaseInfo(new NeuralSparseQueryTwoPhaseInfo(PHASE_ONE, PRUNE_RATIO, PRUNE_TYPE))
            .build();
        final NeuralQueryBuilder copy = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .neuralSparseQueryTwoPhaseInfo(new NeuralSparseQueryTwoPhaseInfo(PHASE_TWO, PRUNE_RATIO, PRUNE_TYPE))
            .modelIdToTwoPhaseSharedQueryTokenSupplier(original::modelIdToTwoPhaseSharedQueryToken)
            .build();

        // prepare data to rewrite on coordinate level
        final QueryCoordinatorContext queryCoordinatorContext = mock(QueryCoordinatorContext.class);
        final IndicesRequest indicesRequest = mock(IndicesRequest.class);
        when(queryCoordinatorContext.convertToCoordinatorContext()).thenReturn(queryCoordinatorContext);
        when(queryCoordinatorContext.getSearchRequest()).thenReturn(indicesRequest);
        when(indicesRequest.indices()).thenReturn(List.of(LOCAL_INDEX_NAME).toArray(new String[0]));
        mockIndexMapping(
            Map.of(
                LOCAL_INDEX_NAME,
                createIndexMappingWithSemanticField(
                    TestSemanticFieldConfig.builder()
                        .modelId(MODEL_ID_1)
                        .searchAnalyzer(SEARCH_ANALYZER)
                        .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                        .build()
                )
            ),
            indicesRequest
        );

        // first rewrite the original query on the coordinate level
        final QueryBuilder rewritten1 = original.doRewrite(queryCoordinatorContext);

        // verify
        // directly rewrite it as NeuralSparseQueryBuilder since we are using search analyzer and we target single index
        assertTrue(rewritten1 instanceof NeuralSparseQueryBuilder);

        // rewrite the copy as part of the rescorer of the query
        final QueryBuilder copyRewritten1 = copy.doRewrite(queryCoordinatorContext);

        // verify
        // directly rewrite it as NeuralSparseQueryBuilder since it's phase 2, and we target one index
        assertTrue(copyRewritten1 instanceof NeuralSparseQueryBuilder);
    }

    @Builder
    @Data
    static class TestSemanticFieldConfig {
        final String modelId;
        final String embeddingFieldType;
        final String semanticInfoName;
        final Boolean chunkingEnabled;
        final String searchAnalyzer;
        final String searchModelId;
    }

    private Map<String, Object> createIndexMappingWithSemanticField(@NonNull final TestSemanticFieldConfig config) {
        String semanticInfoFieldName = FIELD_NAME + "_semantic_info";
        final Map<String, Object> semanticFieldConfig = new HashMap<>();
        semanticFieldConfig.put(TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFieldConfig.put(SemanticFieldConstants.MODEL_ID, config.modelId);
        if (Boolean.TRUE.equals(config.chunkingEnabled)) {
            semanticFieldConfig.put(SemanticFieldConstants.CHUNKING, Boolean.TRUE);
        }
        if (config.semanticInfoName != null) {
            semanticInfoFieldName = config.semanticInfoName;
            semanticFieldConfig.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, config.semanticInfoName);
        }
        if (config.searchAnalyzer != null) {
            semanticFieldConfig.put(SemanticFieldConstants.SEMANTIC_FIELD_SEARCH_ANALYZER, config.searchAnalyzer);
        }
        if (config.searchModelId != null) {
            semanticFieldConfig.put(SemanticFieldConstants.SEARCH_MODEL_ID, config.searchModelId);
        }

        if (Boolean.TRUE.equals(config.chunkingEnabled)) {
            return Map.of(
                PROPERTIES,
                Map.of(
                    FIELD_NAME,
                    semanticFieldConfig,
                    semanticInfoFieldName,
                    Map.of(
                        PROPERTIES,
                        Map.of(
                            SemanticInfoFieldConstants.CHUNKS_FIELD_NAME,
                            Map.of(
                                TYPE,
                                ObjectMapper.NESTED_CONTENT_TYPE,
                                PROPERTIES,
                                Map.of(SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME, Map.of(TYPE, config.embeddingFieldType))
                            )
                        )
                    )
                )
            );
        } else {
            return Map.of(
                PROPERTIES,
                Map.of(
                    FIELD_NAME,
                    semanticFieldConfig,
                    semanticInfoFieldName,
                    Map.of(PROPERTIES, Map.of(SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME, Map.of(TYPE, config.embeddingFieldType)))
                )
            );
        }

    }
}
