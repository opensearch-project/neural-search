/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_FUSION_ENABLED;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.SearchPipelineMetadata;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class DefaultFusionTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";
    private static final String OTHER_INDEX = "other-index";
    private static final String NORMALIZATION_PIPELINE = "norm-pipeline";
    private static final String SCORE_RANKER_PIPELINE = "rrf-pipeline";
    /** A pipeline with only a response processor: it exists, and combines nothing. */
    private static final String RESPONSE_ONLY_PIPELINE = "response-only-pipeline";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initCluster(Version.CURRENT, true, indexDefaults(INDEX, null));
    }

    @Override
    public void tearDown() throws Exception {
        NeuralSearchClusterUtil.instance().initialize(null, null);
        super.tearDown();
    }

    // ---- requests that run with the default fusion ----

    /**
     * Every pipeline state in which core runs no normalization-processor or score-ranker-processor: the request's query is
     * replaced by a copy of the hybrid carrying {@code fusion: {}}, and nothing the caller holds is written.
     */
    @SneakyThrows
    public void testApplyTo_whenNoPipelineCombinesTheScores_thenTheQueryRunsWithTheDefaultFusion() {
        Map<String, Runnable> clusters = new LinkedHashMap<>();
        Map<String, SearchRequest> requests = new LinkedHashMap<>();

        clusters.put("no pipeline anywhere", () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, null)));
        requests.put("no pipeline anywhere", new SearchRequest(INDEX));

        clusters.put(
            "_none over a normalizing index default",
            () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, NORMALIZATION_PIPELINE))
        );
        requests.put("_none over a normalizing index default", new SearchRequest(INDEX).pipeline("_none"));

        clusters.put("a named pipeline that combines nothing", () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, null)));
        requests.put("a named pipeline that combines nothing", new SearchRequest(INDEX).pipeline(RESPONSE_ONLY_PIPELINE));

        clusters.put(
            "an index default that combines nothing",
            () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, RESPONSE_ONLY_PIPELINE))
        );
        requests.put("an index default that combines nothing", new SearchRequest(INDEX));

        clusters.put(
            "index defaults that differ",
            () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, NORMALIZATION_PIPELINE, OTHER_INDEX, SCORE_RANKER_PIPELINE))
        );
        requests.put("index defaults that differ", new SearchRequest(INDEX, OTHER_INDEX));

        clusters.put(
            "[_none, a pipeline that combines nothing]",
            () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, "_none", OTHER_INDEX, RESPONSE_ONLY_PIPELINE))
        );
        requests.put("[_none, a pipeline that combines nothing]", new SearchRequest(INDEX, OTHER_INDEX));

        clusters.put(
            "a request that names no index",
            () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, NORMALIZATION_PIPELINE))
        );
        requests.put("a request that names no index", new SearchRequest());

        clusters.put("a named pipeline core does not know", () -> initCluster(Version.CURRENT, true, indexDefaults(INDEX, null)));
        requests.put("a named pipeline core does not know", new SearchRequest(INDEX).pipeline("missing"));

        for (Map.Entry<String, SearchRequest> entry : requests.entrySet()) {
            String arm = entry.getKey();
            clusters.get(arm).run();
            assertRunsWithTheDefaultFusion(arm, entry.getValue(), source -> {});
        }

        // An inline body is the pipeline: one that combines nothing is no reason to stay classic, and it stays on the copy for
        // core to run.
        initCluster(Version.CURRENT, true, indexDefaults(INDEX, null));
        Map<String, Object> inlineBody = new HashMap<>(Map.of("response_processors", List.of()));
        assertRunsWithTheDefaultFusion(
            "an inline body that combines nothing",
            new SearchRequest(INDEX),
            source -> source.searchPipelineSource(inlineBody)
        );

        // The rescore list is the one thing fused mode rewrites in place, so the copy has its own.
        assertRunsWithTheDefaultFusion(
            "a rescored request",
            new SearchRequest(INDEX),
            source -> source.addRescorer(new QueryRescorerBuilder(QueryBuilders.matchAllQuery()))
        );
    }

    /** A hybrid below the classic one changes nothing here: fused mode's own rewrite judges the nesting. */
    public void testApplyTo_whenTheHybridHoldsAnotherHybrid_thenItStillRunsWithTheDefaultFusion() {
        HybridQueryBuilder classicInClassic = classicHybrid();
        classicInClassic.add(classicHybrid());
        SearchRequest classicRequest = new SearchRequest(INDEX).source(new SearchSourceBuilder().query(classicInClassic));
        assertNotSame(classicRequest, DefaultFusion.applyTo(classicRequest));

        HybridQueryBuilder fusedInClassic = classicHybrid();
        fusedInClassic.add(classicHybrid().fusion(new HashMap<>()));
        SearchRequest fusedRequest = new SearchRequest(INDEX).source(new SearchSourceBuilder().query(fusedInClassic));
        assertNotSame(fusedRequest, DefaultFusion.applyTo(fusedRequest));
    }

    // ---- requests left as they are ----

    @SneakyThrows
    public void testApplyTo_whenAPipelineCombinesTheScores_thenTheRequestIsUntouched() {
        initCluster(Version.CURRENT, true, indexDefaults(INDEX, null));
        assertUntouched("a named normalization pipeline", new SearchRequest(INDEX).pipeline(NORMALIZATION_PIPELINE));
        assertUntouched("a named score-ranker pipeline", new SearchRequest(INDEX).pipeline(SCORE_RANKER_PIPELINE));
        SearchRequest inlineNormalization = new SearchRequest(INDEX);
        assertUntouched(
            "an inline normalization body",
            inlineNormalization,
            source -> source.searchPipelineSource(
                new HashMap<>(Map.of("phase_results_processors", List.of(Map.of("normalization-processor", Map.of()))))
            )
        );
        assertUntouched(
            "an inline body next to a named pipeline, which core refuses",
            new SearchRequest(INDEX).pipeline(RESPONSE_ONLY_PIPELINE),
            source -> source.searchPipelineSource(new HashMap<>())
        );

        initCluster(Version.CURRENT, true, indexDefaults(INDEX, NORMALIZATION_PIPELINE));
        assertUntouched("a normalizing index default", new SearchRequest(INDEX));

        initCluster(Version.CURRENT, true, indexDefaults(INDEX, "_none", OTHER_INDEX, NORMALIZATION_PIPELINE));
        assertUntouched("[_none, a normalizing pipeline]: core runs the second", new SearchRequest(INDEX, OTHER_INDEX));
    }

    @SneakyThrows
    public void testApplyTo_whenTheRequestShapeStaysClassic_thenTheRequestIsUntouched() {
        assertUntouched("size 0, _count included", new SearchRequest(INDEX), source -> source.size(0));
        assertUntouched("scroll", new SearchRequest(INDEX).scroll(TimeValue.timeValueMinutes(1)));
        assertUntouched("a remote index", new SearchRequest("remote:" + INDEX));
        assertUntouched("a remote wildcard alias", new SearchRequest("*:" + INDEX));
        assertUntouched("a cross-cluster sub-request", crossClusterSubRequest(new SearchRequest(INDEX)));
        for (int depth : new int[] { 0, -1 }) {
            assertUntouchedQuery("pagination_depth " + depth, classicHybrid().paginationDepth(depth));
        }
        assertUntouchedQuery("an empty queries list", new HybridQueryBuilder());
        assertUntouchedQuery("a hybrid with fusion", classicHybrid().fusion(new HashMap<>()));
        assertUntouchedQuery("a root that is not a hybrid", QueryBuilders.boolQuery().must(classicHybrid()));

        SearchRequest noSource = new SearchRequest();
        noSource.indices(INDEX);
        assertSame(noSource, DefaultFusion.applyTo(noSource));
        assertNull(noSource.source());
    }

    @SneakyThrows
    public void testApplyTo_whenFusedModeCannotRunOrNothingCanBeDecided_thenTheRequestIsUntouched() {
        initCluster(Version.CURRENT, false, indexDefaults(INDEX, null));
        assertUntouched("fusion mode switched off", new SearchRequest(INDEX));

        initCluster(Version.V_3_9_0, true, indexDefaults(INDEX, null));
        assertUntouched("a node older than fused mode", new SearchRequest(INDEX));

        NeuralSearchClusterUtil.instance().initialize(null, null);
        assertUntouched("no cluster service", new SearchRequest(INDEX));

        // A check that throws: cluster settings that do not register the switch fail every lookup of it.
        initCluster(Version.CURRENT, true, indexDefaults(INDEX, null));
        ClusterService clusterService = NeuralSearchClusterUtil.instance().getClusterService();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, Set.of(MAX_FUSION_LEG_SEARCHES)));
        assertUntouched("a check that throws", new SearchRequest(INDEX));
    }

    // ---- the caller's request ----

    /**
     * The request to run is a copy: the caller's request, source and hybrid stay exactly as written, so a request searched
     * again — a page loop that moves {@code from} on the source it holds, say — is decided and copied again from what it holds
     * now. Everything core reads off the request travels with the copy, the named pipeline and the parent task included.
     */
    public void testApplyTo_whenTheQueryRunsWithTheDefaultFusion_thenTheCallersRequestIsLeftAsWritten() {
        SearchSourceBuilder submitted = new SearchSourceBuilder().query(classicHybrid());
        SearchRequest request = new SearchRequest(INDEX).source(submitted);
        request.pipeline(RESPONSE_ONLY_PIPELINE);
        request.routing("r1");
        request.preference("_local");
        request.requestCache(false);
        request.allowPartialSearchResults(false);
        request.setBatchedReduceSize(7);
        request.setMaxConcurrentShardRequests(3);
        request.setPreFilterShardSize(9);
        request.setCcsMinimizeRoundtrips(false);
        request.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(5));
        request.setPhaseTook(true);
        request.setParentTask(new TaskId("node-1", 42L));
        request.remoteAddress(new TransportAddress(TransportAddress.META_ADDRESS, 9300));

        SearchRequest first = DefaultFusion.applyTo(request);

        assertNotSame(request, first);
        assertSame("the caller's request keeps its source", submitted, request.source());
        assertNull(((HybridQueryBuilder) submitted.query()).fusion());
        assertEquals(Map.of(), defaultFusionQuery(first).fusion());
        assertArrayEquals(request.indices(), first.indices());
        assertEquals(request.indicesOptions(), first.indicesOptions());
        assertEquals(RESPONSE_ONLY_PIPELINE, first.pipeline());
        assertEquals(request.routing(), first.routing());
        assertEquals(request.preference(), first.preference());
        assertEquals(request.requestCache(), first.requestCache());
        assertEquals(request.allowPartialSearchResults(), first.allowPartialSearchResults());
        assertEquals(request.getBatchedReduceSize(), first.getBatchedReduceSize());
        assertEquals(request.getMaxConcurrentShardRequests(), first.getMaxConcurrentShardRequests());
        assertEquals(request.getPreFilterShardSize(), first.getPreFilterShardSize());
        assertEquals(request.isCcsMinimizeRoundtrips(), first.isCcsMinimizeRoundtrips());
        assertEquals(request.getCancelAfterTimeInterval(), first.getCancelAfterTimeInterval());
        assertEquals(request.isPhaseTook(), first.isPhaseTook());
        assertEquals(request.searchType(), first.searchType());
        assertEquals(request.getParentTask(), first.getParentTask());
        assertEquals(request.remoteAddress(), first.remoteAddress());

        // The next page, written where the caller holds it, is what the next search runs.
        request.source().from(10);
        SearchRequest second = DefaultFusion.applyTo(request);
        assertNotSame(first.source(), second.source());
        assertEquals(10, second.source().from());

        // A normalizing pipeline named since keeps the next search classic, on the caller's own request.
        request.pipeline(NORMALIZATION_PIPELINE);
        assertSame(request, DefaultFusion.applyTo(request));
        assertSame(submitted, request.source());
    }

    /** Two requests of one _msearch built in code may share a source; each runs a copy of its own. */
    public void testApplyTo_whenTwoRequestsShareOneSource_thenEachGetsItsOwnCopy() {
        SearchSourceBuilder shared = new SearchSourceBuilder().query(classicHybrid());
        SearchRequest first = DefaultFusion.applyTo(new SearchRequest(INDEX).source(shared));
        SearchRequest second = DefaultFusion.applyTo(new SearchRequest(INDEX).source(shared));
        assertNotSame(first.source(), second.source());
        assertNotSame(first.source().query(), second.source().query());
        assertNull(((HybridQueryBuilder) shared.query()).fusion());
    }

    /** Core's mark is read; a getter that is missing, or that throws, counts as a sub-request, which keeps the hybrid classic. */
    @SneakyThrows
    public void testIsCrossClusterSubRequest_thenReadsCoresMarkAndFailsClosed() {
        assertFalse(DefaultFusion.isCrossClusterSubRequest(new SearchRequest(INDEX)));
        assertTrue(DefaultFusion.isCrossClusterSubRequest(crossClusterSubRequest(new SearchRequest(INDEX))));

        assertNull(DefaultFusion.searchRequestGetter("noSuchGetter"));
        assertTrue(DefaultFusion.isCrossClusterSubRequest(new SearchRequest(INDEX), null));
        MethodHandle throwing = MethodHandles.dropArguments(
            MethodHandles.throwException(String.class, IllegalStateException.class).bindTo(new IllegalStateException("unreadable")),
            0,
            SearchRequest.class
        );
        assertTrue(DefaultFusion.isCrossClusterSubRequest(new SearchRequest(INDEX), throwing));
    }

    // ---- helpers ----

    /** A classic hybrid with every request-level option set, so that the copy can be checked to carry each of them. */
    private static HybridQueryBuilder classicHybrid() {
        HybridQueryBuilder hybrid = new HybridQueryBuilder();
        hybrid.add(QueryBuilders.termQuery("text", "hello"));
        hybrid.add(QueryBuilders.matchQuery("text", "world"));
        hybrid.queryName("named-hybrid");
        hybrid.boost(2.5f);
        hybrid.paginationDepth(30);
        hybrid.weights(List.of(0.4f, 0.6f));
        return hybrid;
    }

    private static HybridQueryBuilder defaultFusionQuery(final SearchRequest request) {
        return (HybridQueryBuilder) request.source().query();
    }

    private static void assertRunsWithTheDefaultFusion(
        final String arm,
        final SearchRequest request,
        final Consumer<SearchSourceBuilder> sourceSetup
    ) {
        HybridQueryBuilder submittedHybrid = classicHybrid();
        SearchSourceBuilder submitted = new SearchSourceBuilder().query(submittedHybrid);
        sourceSetup.accept(submitted);
        List<?> submittedRescores = submitted.rescores();
        request.source(submitted);

        SearchRequest routed = DefaultFusion.applyTo(request);

        assertNotSame(arm, request, routed);
        assertSame(arm + ": the caller's request keeps its source", submitted, request.source());
        assertNotSame(arm, submitted, routed.source());
        assertEquals(arm, request.pipeline(), routed.pipeline());
        HybridQueryBuilder copy = defaultFusionQuery(routed);
        assertEquals(arm, Map.of(), copy.fusion());
        assertTrue(arm + ": a mutable block, as the parser builds", copy.fusion() instanceof HashMap);
        assertSame(arm, submitted, copy.submittedSource());
        assertEquals(arm, submittedHybrid.queries().size(), copy.queries().size());
        for (int i = 0; i < copy.queries().size(); i++) {
            assertSame(arm + ": sub-query " + i, submittedHybrid.queries().get(i), copy.queries().get(i));
        }
        assertEquals(arm, submittedHybrid.queryName(), copy.queryName());
        assertEquals(arm, submittedHybrid.boost(), copy.boost(), 0.0f);
        assertEquals(arm, submittedHybrid.paginationDepth(), copy.paginationDepth());
        assertEquals(arm, submittedHybrid.weights(), copy.weights());
        assertSame(
            arm + ": the inline body stays for core to run",
            submitted.searchPipelineSource(),
            routed.source().searchPipelineSource()
        );
        assertEquals(arm, submitted.size(), routed.source().size());
        // The caller's objects are as written.
        assertSame(arm, submittedHybrid, submitted.query());
        assertNull(arm, submittedHybrid.fusion());
        assertNull(arm, submittedHybrid.submittedSource());
        assertSame(arm, submittedRescores, submitted.rescores());
        if (Objects.nonNull(submitted.rescores())) {
            assertNotSame(arm + ": a rescore list of its own", submitted.rescores(), routed.source().rescores());
            assertEquals(arm, submitted.rescores(), routed.source().rescores());
        }
    }

    private static void assertUntouched(final String arm, final SearchRequest request) {
        assertUntouched(arm, request, source -> {});
    }

    private static void assertUntouched(final String arm, final SearchRequest request, final Consumer<SearchSourceBuilder> sourceSetup) {
        HybridQueryBuilder submittedHybrid = classicHybrid();
        SearchSourceBuilder submitted = new SearchSourceBuilder().query(submittedHybrid);
        sourceSetup.accept(submitted);
        request.source(submitted);
        assertSame(arm, request, DefaultFusion.applyTo(request));
        assertSame(arm, submitted, request.source());
        assertSame(arm, submittedHybrid, submitted.query());
        assertNull(arm, submittedHybrid.fusion());
    }

    private static void assertUntouchedQuery(final String arm, final QueryBuilder query) {
        SearchSourceBuilder submitted = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest(INDEX).source(submitted);
        assertSame(arm, request, DefaultFusion.applyTo(request));
        assertSame(arm, submitted, request.source());
        assertSame(arm, query, submitted.query());
    }

    /** Core's copy of a request for one remote cluster, built the way core's cross-cluster search builds it. */
    @SneakyThrows
    @SuppressForbidden(reason = "SearchRequest#subSearchRequest is package-private; it is the only way core builds a sub-request")
    private static SearchRequest crossClusterSubRequest(final SearchRequest original) {
        Method subSearchRequest = SearchRequest.class.getDeclaredMethod(
            "subSearchRequest",
            SearchRequest.class,
            String[].class,
            String.class,
            long.class,
            boolean.class
        );
        subSearchRequest.setAccessible(true);
        return (SearchRequest) subSearchRequest.invoke(null, original, original.indices(), "remote", 0L, false);
    }

    /** Index name to its {@code index.search.default_pipeline}, in order; a null value declares none. */
    private static Map<String, String> indexDefaults(final String... indexAndDefault) {
        Map<String, String> defaults = new LinkedHashMap<>();
        for (int i = 0; i < indexAndDefault.length; i += 2) {
            defaults.put(indexAndDefault[i], indexAndDefault[i + 1]);
        }
        return defaults;
    }

    /**
     * Wire NeuralSearchClusterUtil to a cluster whose nodes are at {@code minNodeVersion}, whose fusion switch is
     * {@code fusionEnabled}, which holds the three test pipelines, and whose indices declare the given default pipelines. The
     * index resolver answers a request with the indices it names, in order.
     */
    @SneakyThrows
    private static void initCluster(final Version minNodeVersion, final boolean fusionEnabled, final Map<String, String> indexDefaults) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.getMinNodeVersion()).thenReturn(minNodeVersion);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.builder().put(HYBRID_FUSION_ENABLED.getKey(), fusionEnabled).build(),
                Set.of(HYBRID_FUSION_ENABLED, MAX_FUSION_LEG_SEARCHES)
            )
        );
        when(metadata.custom(SearchPipelineMetadata.TYPE)).thenReturn(
            new SearchPipelineMetadata(
                Map.of(
                    NORMALIZATION_PIPELINE,
                    pipeline(NORMALIZATION_PIPELINE, "phase_results_processors", "normalization-processor"),
                    SCORE_RANKER_PIPELINE,
                    pipeline(SCORE_RANKER_PIPELINE, "phase_results_processors", "score-ranker-processor"),
                    RESPONSE_ONLY_PIPELINE,
                    pipeline(RESPONSE_ONLY_PIPELINE, "response_processors", "rename_field")
                )
            )
        );
        Map<String, Index> indices = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : indexDefaults.entrySet()) {
            Index index = new Index(entry.getKey(), "uuid-" + entry.getKey());
            indices.put(entry.getKey(), index);
            Settings.Builder settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.version.created", Version.CURRENT.id);
            if (Objects.nonNull(entry.getValue())) {
                settings.put("index.search.default_pipeline", entry.getValue());
            }
            when(metadata.index(index)).thenReturn(IndexMetadata.builder(entry.getKey()).settings(settings.build()).build());
        }
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenAnswer(invocation -> {
            IndicesRequest request = invocation.getArgument(1);
            String[] names = Objects.isNull(request.indices()) || request.indices().length == 0
                ? indices.keySet().toArray(new String[0])
                : request.indices();
            return Arrays.stream(names).map(indices::get).toArray(Index[]::new);
        });
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    @SneakyThrows
    private static PipelineConfiguration pipeline(final String id, final String processorsKey, final String processorType) {
        BytesReference body = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .startArray(processorsKey)
                .startObject()
                .startObject(processorType)
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        return new PipelineConfiguration(id, body, MediaTypeRegistry.JSON);
    }
}
