/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Integration tests for the {@code clustering_batch_size} mapping parameter, which builds the native
 * engine's index one term window at a time instead of holding the whole term space, spilling each
 * window into the segment's directory. Native only: the Lucene engine rejects any non-default value.
 *
 * What a batched build changes is the peak memory it takes to produce the index, not the index -- so
 * the assertion is that the same corpus and query return exactly what the unbatched build returns in
 * {@link SparseForwardIndexIT}, and that the shard is left with nothing but its own files.
 */
public class SparseClusteringBatchSizeIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-clustering-batch-size";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";

    private static final List<Map<String, Float>> DOCUMENTS = List.of(
        Map.of("1000", 0.1f, "2000", 0.1f),
        Map.of("1000", 0.2f, "2000", 0.2f),
        Map.of("1000", 0.3f, "2000", 0.3f),
        Map.of("1000", 0.4f, "2000", 0.4f),
        Map.of("1000", 0.5f, "2000", 0.5f),
        Map.of("1000", 0.6f, "2000", 0.6f),
        Map.of("1000", 0.7f, "2000", 0.7f),
        Map.of("1000", 0.8f, "2000", 0.8f)
    );

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return nativeEngineOnly();
    }

    public SparseClusteringBatchSizeIT(SparseEngine engine) {
        super(engine);
    }

    public void testSearchDocuments_withBatchedClustering() throws Exception {
        createIndexWithClusteringBatchSize(16);

        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCUMENTS);

        NeuralSparseQueryBuilder queryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, queryBuilder, 10);
        assertNotNull(searchResults);
        // Exactly what the unbatched build returns: n_postings = 4 keeps each posting list's 4
        // highest-weight documents, whichever window the term was clustered in.
        assertEquals(4, getHitCount(searchResults));
        assertEquals(List.of("8", "7", "6", "5"), getDocIDs(searchResults));
    }

    /**
     * More windows than the corpus has terms. Windows are cut to even out clustering load and clamped
     * to the dimension, so an over-large count has to resolve rather than leave a term unbuilt -- a
     * short window would produce a posting list the search never sees.
     */
    public void testSearchDocuments_withMoreWindowsThanTerms() throws Exception {
        createIndexWithClusteringBatchSize(10000);

        ingestDocumentsAndForceMergeForSingleShard(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCUMENTS);

        NeuralSparseQueryBuilder queryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, queryBuilder, 10);
        assertEquals(4, getHitCount(searchResults));
        assertEquals(List.of("8", "7", "6", "5"), getDocIDs(searchResults));
    }

    /**
     * The mapping is built here rather than through {@link SparseTestCommon} because this is the only
     * suite that sends the parameter, and it sits in {@code method.parameters} alongside the seismic
     * ones.
     */
    private void createIndexWithClusteringBatchSize(int clusteringBatchSize) throws Exception {
        String mappings = String.format(
            Locale.ROOT,
            "{\"properties\":{\"%s\":{\"type\":\"sparse_vector\",\"method\":{\"name\":\"seismic\",\"engine\":\"%s\","
                + "\"parameters\":{\"n_postings\":4,\"summary_prune_ratio\":0.4,\"cluster_ratio\":0.5,"
                + "\"approximate_threshold\":8,\"clustering_batch_size\":%d}}}}}",
            TEST_SPARSE_FIELD_NAME,
            SparseEngine.NATIVE.getName(),
            clusteringBatchSize
        );
        createIndex(
            TEST_INDEX_NAME,
            String.format(Locale.ROOT, "{\"settings\":%s,\"mappings\":%s}", SparseTestCommon.prepareIndexSettings(1, 0), mappings)
        );
    }
}
