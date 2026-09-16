/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class ReturnedEmbeddingFieldsTests extends OpenSearchTestCase {

    private static final String VECTOR_INDEX = "vectors";
    private static final String TEXT_INDEX = "texts";

    private static final String VECTOR_MAPPING =
        "{\"properties\":{\"text\":{\"type\":\"text\"},\"vec\":{\"type\":\"knn_vector\",\"dimension\":768},"
            + "\"emb\":{\"properties\":{\"inner\":{\"type\":\"knn_vector\",\"dimension\":2},\"label\":{\"type\":\"keyword\"}}}}}";
    private static final String TEXT_MAPPING = "{\"properties\":{\"text\":{\"type\":\"text\"},\"num\":{\"type\":\"integer\"}}}";
    private static final String SPARSE_MAPPING = "{\"properties\":{\"text\":{\"type\":\"text\"},\"sparse\":{\"type\":\"rank_features\"}}}";

    private final Map<Index, IndexMetadata> indices = new LinkedHashMap<>();

    @Before
    public void resetCache() {
        ReturnedEmbeddingFields.clearCache();
        ObservedSourceSizes.clear();
        indices.clear();
    }

    /** Seed what a response of this request shape would have taught the gate: {@code bytes} of _source per hit of {@code index}. */
    private static void observe(String index, SearchSourceBuilder shape, long bytes) {
        SearchHit hit = new SearchHit(0, "1", null, null);
        hit.shard(new SearchShardTarget("node", new ShardId(index, "uuid", 0), null, OriginalIndices.NONE));
        // A real JSON document of exactly `bytes` bytes: sourceRef sniffs the bytes for a compressor, so they must be XContent.
        String frame = "{\"body\":\"\"}";
        hit.sourceRef(new BytesArray(frame.substring(0, 9) + "x".repeat((int) Math.max(0, bytes - frame.length())) + frame.substring(9)));
        ObservedSourceSizes.record(shape, new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f));
    }

    /** The cluster state holds the given indices; the resolver resolves every request to all of them, in order. */
    private void cluster(IndexMetadata... metadata) {
        indices.clear();
        Metadata clusterMetadata = mock(Metadata.class);
        ClusterState state = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(clusterMetadata);
        for (IndexMetadata index : metadata) {
            indices.put(index.getIndex(), index);
            when(clusterMetadata.index(index.getIndex())).thenReturn(index);
        }
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(
            indices.keySet().toArray(Index[]::new)
        );
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
    }

    private static IndexMetadata index(String name, String mapping, long mappingVersion) {
        return index(name, mapping, mappingVersion, null);
    }

    @SneakyThrows
    private static IndexMetadata index(String name, String mapping, long mappingVersion, String sourceSpec) {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT.id)
            .put("index.uuid", name + "-uuid")
            .build();
        IndexMetadata.Builder builder = IndexMetadata.builder(name).settings(settings).mappingVersion(mappingVersion);
        if (mapping != null) {
            String withSource = sourceSpec == null ? mapping : "{\"_source\":" + sourceSpec + "," + mapping.substring(1);
            builder.putMapping(withSource);
        }
        return builder.build();
    }

    private static SearchRequest request(SearchSourceBuilder source) {
        return new SearchRequest(VECTOR_INDEX).source(source);
    }

    private static SearchSourceBuilder source() {
        return new SearchSourceBuilder().query(new MatchAllQueryBuilder());
    }

    /** Would the page carry any embedding payload (or can that not be established)? The presence view of the estimate. */
    private static boolean carried(SearchRequest request) {
        return ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request) > 0;
    }

    public void testEstimatedBytes_whenVectorsSurviveTheFilters_thenTheSumOfTheirDeclaredWidths() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        assertEquals("768 floats + 2 floats", 7680L + 20L, ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request(source())));
        assertEquals(
            "only the nested one left",
            20L,
            ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request(source().fetchSource(null, new String[] { "vec" })))
        );
        assertEquals(
            "fields adds the big one back",
            7700L,
            ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(
                request(source().fetchSource(null, new String[] { "vec" }).fetchField("vec"))
            )
        );
        assertEquals(0L, ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request(source().fetchSource(false))));
    }

    public void testEstimatedBytes_perFieldDeclaration() {
        assertEquals(7680L, ReturnedEmbeddingFields.estimatedBytes("knn_vector", Map.of("dimension", 768)));
        assertEquals(
            "byte vectors are narrower",
            768L * 4,
            ReturnedEmbeddingFields.estimatedBytes("knn_vector", Map.of("dimension", 768, "data_type", "byte"))
        );
        assertEquals(
            "binary dimensions are bits",
            768L,
            ReturnedEmbeddingFields.estimatedBytes("knn_vector", Map.of("dimension", 768, "data_type", "binary"))
        );
        assertEquals(
            "model-bound vector without a dimension",
            1024L * 10,
            ReturnedEmbeddingFields.estimatedBytes("knn_vector", Map.of("model_id", "m"))
        );
        assertEquals(4096L, ReturnedEmbeddingFields.estimatedBytes("rank_features", Map.of()));
    }

    /**
     * The measured matrix, reproduced by the decision: 2 legs, window 100, 768-dim vectors in {@code _source}. At
     * {@code size:100} the fast path fetches 100 extra documents (~0.77 MB estimated) and stays on; at {@code size:10} it
     * would fetch 190 extra (~1.46 MB) and is refused. Without the vector, or with {@code _source} off, never refused.
     */
    public void testFastPathFetchExceedsBudget_reproducesTheMeasuredCrossover() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        // The gate weighs _source by what responses of this shape returned. Seed the observation the measured index
        // produced — a 768-dim float vector serialized as JSON text, ~7.7 KB per document — for the unfiltered shape.
        observe(VECTOR_INDEX, source(), 768 * ReturnedEmbeddingFields.FLOAT_VECTOR_BYTES_PER_DIMENSION);
        assertFalse(
            "size 100: 100 extra documents × 7.7 KB < 1 MB",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(100)), 2, 100)
        );
        assertTrue(
            "size 10: 190 extra documents × 7.7 KB > 1 MB",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(10)), 2, 100)
        );
        assertTrue("default size is 10", ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source()), 2, 100));
        assertTrue(
            "a wider window at size 100 crosses too",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(100)), 2, 500)
        );
        // Excluding the vectors changes the _source filter shape, and the size under that shape is its own observation:
        // the text that is left, a few hundred bytes.
        SearchSourceBuilder excludingVectors = source().size(10).fetchSource(null, new String[] { "vec", "emb.inner" });
        observe(VECTOR_INDEX, excludingVectors, 300);
        assertFalse(
            "vector excluded: only the observed text is weighed",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(excludingVectors), 2, 500)
        );
        assertFalse(
            "_source off",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(10).fetchSource(false)), 2, 500)
        );
        assertFalse(
            "size ≥ legs × window: nothing extra at all",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(200)), 2, 100)
        );
        // unresolvable: fail closed
        IndexNameExpressionResolver failing = mock(IndexNameExpressionResolver.class);
        when(failing.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenThrow(new IndexNotFoundException("missing"));
        NeuralSearchClusterUtil.instance().initialize(NeuralSearchClusterUtil.instance().getClusterService(), failing);
        assertTrue(ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(100)), 2, 100));
        assertFalse(
            "but never a lookup when _source is off",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().fetchSource(false)), 2, 100)
        );
    }

    public void testRequested_whenSourceOffAndNoFields_thenFalseWithoutAnyLookup() {
        // no cluster util set up at all: a lookup would fail closed, so false here proves there was none
        NeuralSearchClusterUtil.instance().initialize(null, null);
        assertFalse(carried(request(source().fetchSource(false))));
    }

    public void testRequested_whenSourceOnAndMappingHasAVector_thenTrue() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        assertTrue("default _source", carried(request(source())));
        assertTrue("explicit _source: true", carried(request(source().fetchSource(true))));
        assertTrue("no source builder at all means core's default, _source on", carried(new SearchRequest(VECTOR_INDEX)));
    }

    public void testRequested_whenSourceOnAndNoEmbeddingInMapping_thenFalse() {
        cluster(index(TEXT_INDEX, TEXT_MAPPING, 1));
        assertFalse(carried(request(source())));
    }

    public void testRequested_whenRequestExcludesOrIncludesAroundTheVectors_thenFalse() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        assertFalse("both vectors excluded", carried(request(source().fetchSource(null, new String[] { "vec", "emb.inner" }))));
        assertFalse("wildcard exclude on the object", carried(request(source().fetchSource(null, new String[] { "vec", "emb.*" }))));
        assertFalse("includes naming only text", carried(request(source().fetchSource(new String[] { "text", "emb.label" }, null))));
        assertTrue("excluding one of two vectors is not enough", carried(request(source().fetchSource(null, new String[] { "vec" }))));
        assertTrue(
            "an include of the parent object keeps the nested vector",
            carried(request(source().fetchSource(new String[] { "emb" }, null)))
        );
        assertTrue(
            "an exclude that misses keeps the vector",
            carried(request(source().fetchSource(null, new String[] { "emb.label", "v" })))
        );
    }

    public void testRequested_whenMappingLevelSourceDropsTheVectors_thenFalse() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1, "{\"excludes\":[\"vec\",\"emb.inner\"]}"));
        assertFalse("mapping excludes", carried(request(source())));
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 2, "{\"includes\":[\"text\"]}"));
        assertFalse("mapping includes", carried(request(source())));
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 3, "{\"enabled\":false}"));
        assertFalse("no _source stored at all", carried(request(source())));
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 4, "{\"excludes\":[\"vec\"]}"));
        assertTrue("mapping drops one vector, the other still ships", carried(request(source())));
    }

    public void testRequested_whenFieldsOrDocValueFieldsNameAnEmbedding_thenTrueEvenWithSourceOff() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        assertTrue("fields: vec", carried(request(source().fetchSource(false).fetchField("vec"))));
        assertTrue("fields wildcard", carried(request(source().fetchSource(false).fetchField("emb.*"))));
        assertTrue("docvalue_fields", carried(request(source().fetchSource(false).docValueField("v*"))));
        assertFalse("fields naming only text", carried(request(source().fetchSource(false).fetchField("text").docValueField("emb.label"))));
    }

    public void testRequested_whenMappingHasRankFeatures_thenTrue() {
        cluster(index(TEXT_INDEX, SPARSE_MAPPING, 1));
        assertTrue(carried(request(source())));
        assertFalse(carried(request(source().fetchSource(null, new String[] { "sparse" }))));
    }

    public void testRequested_whenAnyTargetedIndexShipsAVector_thenTrue() {
        cluster(index(TEXT_INDEX, TEXT_MAPPING, 1), index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        assertTrue(carried(new SearchRequest(TEXT_INDEX, VECTOR_INDEX).source(source())));
        cluster(index(TEXT_INDEX, TEXT_MAPPING, 1), index("other", TEXT_MAPPING, 1));
        assertFalse(carried(new SearchRequest(TEXT_INDEX, "other").source(source())));
    }

    public void testRequested_whenIndicesCannotBeResolvedOrHaveNoMapping_thenFailsClosedOrPasses() {
        cluster(index(TEXT_INDEX, TEXT_MAPPING, 1));
        IndexNameExpressionResolver failing = mock(IndexNameExpressionResolver.class);
        when(failing.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenThrow(new IndexNotFoundException("missing"));
        NeuralSearchClusterUtil.instance().initialize(NeuralSearchClusterUtil.instance().getClusterService(), failing);
        assertTrue("unresolvable index: fail closed", carried(request(source())));

        IndexNameExpressionResolver empty = mock(IndexNameExpressionResolver.class);
        when(empty.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(new Index[0]);
        NeuralSearchClusterUtil.instance().initialize(NeuralSearchClusterUtil.instance().getClusterService(), empty);
        assertTrue("nothing resolved: fail closed", carried(request(source())));
        assertEquals(
            "the fetch estimate fails closed the same way",
            Long.MAX_VALUE,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source()))
        );

        NeuralSearchClusterUtil.instance().initialize(null, null);
        assertTrue("no cluster util at all: fail closed", carried(request(source())));

        cluster(index(TEXT_INDEX, null, 1));
        assertFalse("an index without a mapping ships no embedding", carried(request(source())));
    }

    public void testRequested_whenAResolvedIndexIsGoneFromTheState_thenFailsClosed() {
        cluster(index(TEXT_INDEX, TEXT_MAPPING, 1));
        IndexNameExpressionResolver stale = mock(IndexNameExpressionResolver.class);
        when(stale.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(
            new Index[] { new Index("vanished", "vanished-uuid") }
        );
        NeuralSearchClusterUtil.instance().initialize(NeuralSearchClusterUtil.instance().getClusterService(), stale);
        assertTrue(carried(request(source())));
        assertEquals(Long.MAX_VALUE, ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source())));
    }

    public void testRequested_whenTheMappingChanges_thenTheCacheFollowsTheMappingVersion() {
        cluster(index(VECTOR_INDEX, TEXT_MAPPING, 1));
        assertFalse(carried(request(source())));
        // same index, a vector field added: a new mapping version invalidates the cached facts
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 2));
        assertTrue(carried(request(source())));
        // and the same version is served from the cache (a mapping the cache never saw would say false)
        cluster(index(VECTOR_INDEX, TEXT_MAPPING, 2));
        assertTrue(carried(request(source())));
    }

    public void testEstimatedBytes_whenTheMappingCannotBeParsed_thenFailsClosed() {
        // metadata whose mapping() throws on sourceAsMap is impractical to build; an index resolved but missing from the
        // state exercises the same fail-closed answer, and a corrupt-mapping variant is covered through a mock that throws
        Index brokenIndex = new Index("broken", "broken-uuid");
        IndexMetadata broken = mock(IndexMetadata.class);
        when(broken.getIndex()).thenReturn(brokenIndex);
        when(broken.mapping()).thenThrow(new IllegalStateException("unparsable"));
        Metadata clusterMetadata = mock(Metadata.class);
        ClusterState state = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(clusterMetadata);
        when(clusterMetadata.index(brokenIndex)).thenReturn(broken);
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenReturn(new Index[] { brokenIndex });
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
        assertEquals(Long.MAX_VALUE, ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request(source())));
        // the fetch estimate reads the mapping only for the `fields` term; with _source off and a field named, it fails
        // closed on the same unparsable mapping
        assertEquals(
            Long.MAX_VALUE,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source().fetchSource(false).docValueField("vec")))
        );
    }

    public void testRequested_whenTheRequestHasNoSourceBuilderAndFieldsAreChecked_thenPatternsAreEmpty() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        // no SearchSourceBuilder at all: _source defaults on, no field patterns — the vector still counts through _source
        assertTrue(carried(new SearchRequest(VECTOR_INDEX)));
    }

    public void testRequested_whenAMappingCarriesNonMapAndStringPatternShapes_thenTheyParse() {
        // exercises: a non-map property entry (ignored), a mapping-level _source.includes as a single string
        String mapping = "{\"properties\":{\"text\":{\"type\":\"text\"},\"vec\":{\"type\":\"knn_vector\",\"dimension\":8}}}";
        cluster(index(VECTOR_INDEX, mapping, 1, "{\"includes\":\"text\"}"));
        assertFalse("a single-string include names only text", carried(request(source())));
    }

    public void testEstimatedBytes_whenTheRequestNeverHadASource_thenSourceDefaultsOnAndPatternsAreEmpty() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        // the no-arg constructor is the one that leaves source null (the String... constructor initializes one)
        assertTrue(carried(new SearchRequest()));
    }

    public void testRequested_whenAPropertyEntryIsNotAMap_thenItIsIgnored() {
        String mapping = "{\"properties\":{\"oddity\":\"not-a-map\",\"vec\":{\"type\":\"knn_vector\",\"dimension\":8}}}";
        cluster(index(VECTOR_INDEX, mapping, 1));
        assertTrue("the vector is still found past the odd entry", carried(request(source())));
    }

    public void testFactsCache_whenCapacityIsReached_thenItIsClearedAndAnswersStayCorrect() {
        Metadata clusterMetadata = mock(Metadata.class);
        ClusterState state = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(clusterMetadata);
        Map<Index, IndexMetadata> byIndex = new LinkedHashMap<>();
        when(clusterMetadata.index(any(Index.class))).thenAnswer(invocation -> byIndex.get(invocation.getArgument(0, Index.class)));
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AtomicReference<Index> current = new AtomicReference<>();
        when(resolver.concreteIndices(any(ClusterState.class), any(IndicesRequest.class))).thenAnswer(
            invocation -> new Index[] { current.get() }
        );
        NeuralSearchClusterUtil.instance().initialize(clusterService, resolver);
        // one past the cache capacity: the clear-at-capacity branch runs and answers stay correct afterwards
        for (int i = 0; i <= 4096; i++) {
            IndexMetadata indexMetadata = index("idx-" + i, i == 4096 ? VECTOR_MAPPING : TEXT_MAPPING, 1);
            byIndex.put(indexMetadata.getIndex(), indexMetadata);
            current.set(indexMetadata.getIndex());
            long bytes = ReturnedEmbeddingFields.estimatedEmbeddingBytesPerDocument(request(source()));
            assertEquals("idx-" + i, i == 4096, bytes > 0);
        }
    }

    public void testEmbeddingFieldTypes() {
        assertEquals(2, ReturnedEmbeddingFields.EMBEDDING_FIELD_TYPES.size());
        assertTrue(ReturnedEmbeddingFields.EMBEDDING_FIELD_TYPES.containsAll(Arrays.asList("knn_vector", "rank_features")));
    }

    // ---- the observed _source term ----

    public void testFetchBytes_whenSourceOnAndUnobserved_thenFailsClosed() {
        cluster(index("text-only", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1));
        assertEquals(
            "no response of this shape seen yet: unknown, two rounds",
            Long.MAX_VALUE,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source().size(10)))
        );
        assertTrue(ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(10)), 2, 100));
    }

    public void testFetchBytes_whenTextIsObserved_thenTheGateWeighsIt() {
        cluster(index("text-only", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1));
        // ~3.2 KB documents (the Quora corpus): 190 extra × 3.2 KB ≈ 0.6 MB, under budget
        observe("text-only", source(), 3_200);
        assertEquals(3_200L, ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source().size(10))));
        assertFalse("small text: armed", ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(10)), 2, 100));
        // 50 KB articles: 190 × 50 KB ≈ 9.5 MB — the case the mapping cannot see
        ObservedSourceSizes.clear();
        observe("text-only", source(), 50_000);
        assertTrue("large text: refused", ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(10)), 2, 100));
        assertFalse(
            "but a page as wide as the window has nothing extra to fetch",
            ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().size(200)), 2, 100)
        );
    }

    public void testFetchBytes_whenSourceOff_thenObservationsAreIrrelevant() {
        cluster(index("text-only", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1));
        observe("text-only", source(), 50_000);
        assertEquals(0L, ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source().fetchSource(false))));
        assertFalse(ReturnedEmbeddingFields.fastPathFetchExceedsBudget(request(source().fetchSource(false)), 2, 100));
    }

    public void testFetchBytes_whenObservedSourceAndFieldsNameAVector_thenBothTermsAdd() {
        cluster(index(VECTOR_INDEX, VECTOR_MAPPING, 1));
        // _source excludes the vectors and weighs 300 bytes; `fields` pulls the 768-dim vector back through docvalues.
        SearchSourceBuilder shape = source().size(10).fetchSource(null, new String[] { "vec", "emb.inner" }).docValueField("vec");
        observe(VECTOR_INDEX, shape, 300);
        assertEquals(
            300 + 768 * ReturnedEmbeddingFields.FLOAT_VECTOR_BYTES_PER_DIMENSION,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(shape))
        );
    }

    public void testFetchBytes_whenObservedUnderAnotherShape_thenStillUnknownForThisOne() {
        cluster(index("text-only", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1));
        observe("text-only", source().fetchSource(new String[] { "title" }, null), 120);
        assertEquals(
            "a different _source filter is a different size: not observed",
            Long.MAX_VALUE,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source()))
        );
        assertEquals(
            120L,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source().fetchSource(new String[] { "title" }, null)))
        );
    }

    public void testFetchBytes_whenSeveralIndicesAreObserved_thenTheWidestCounts() {
        cluster(
            index("small", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1),
            index("large", "{\"properties\":{\"body\":{\"type\":\"text\"}}}", 1)
        );
        observe("small", source(), 500);
        observe("large", source(), 20_000);
        assertEquals(20_000L, ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source())));
        ObservedSourceSizes.clear();
        observe("small", source(), 500);
        assertEquals(
            "one index unobserved: unknown",
            Long.MAX_VALUE,
            ReturnedEmbeddingFields.estimatedFetchBytesPerDocument(request(source()))
        );
    }
}
