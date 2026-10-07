/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for a replicated sparse index.
 *
 * <p>The native engine writes its index to a {@code .nsparse} file that has to be part of the
 * segment's file set, with the Lucene footer {@code DefaultNativeIndexWriter} appends, for peer
 * recovery to carry it to the replica. When that breaks, the replica ends up with a segment whose
 * engine file is missing and answers with a shard failure or with nothing - never with visibly wrong
 * scores - so only a query that is routed at the replica on purpose can catch it.
 *
 * <p>Requires two or more data nodes: with one node the replica stays unassigned and every
 * assertion below is vacuous. CI covers this through the {@code integMultiNodeTest} job
 * ({@code -PnumNodes=3}); a local single-node {@code ./gradlew integTest} run skips it.
 */
public class SparseReplicaIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-replica";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final int SHARDS = 1;
    private static final int REPLICAS = 1;
    private static final int DOC_COUNT = 8;

    private static final List<Map<String, Float>> DOCS = List.of(
        Map.of("1000", 0.1f, "2000", 0.1f),
        Map.of("1000", 0.2f, "2000", 0.2f),
        Map.of("1000", 0.3f, "2000", 0.3f),
        Map.of("1000", 0.4f, "2000", 0.4f),
        Map.of("1000", 0.5f, "2000", 0.5f),
        Map.of("1000", 0.6f, "2000", 0.6f),
        Map.of("1000", 0.7f, "2000", 0.7f),
        Map.of("1000", 0.8f, "2000", 0.8f)
    );

    private static final Map<String, Float> QUERY_TOKENS = Map.of("1000", 0.1f, "2000", 0.2f);

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return allEngines();
    }

    public SparseReplicaIT(SparseEngine engine) {
        super(engine);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue(
            "a replica needs a second data node; run with -PnumNodes=2 or more",
            SparseTestCommon.getDataNodeCount(client()) >= REPLICAS + 1
        );
    }

    /** The default forward index layout, which is the only one the Lucene engine has. */
    public void testPrimaryAndReplicaBothServeTheSparseIndex() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, DOC_COUNT, SHARDS, REPLICAS);
        assertPrimaryAndReplicaAgree();
    }

    /**
     * per_block is where an unshipped engine file is most likely to go unnoticed: a disk-resident
     * index reads only the blocks a query selects, so a truncated or absent file can still answer
     * some queries.
     */
    public void testPrimaryAndReplicaBothServeAPerBlockSparseIndex() throws Exception {
        assumeTrue("forward_index is a native-only mapping parameter", SparseEngine.NATIVE == engine);
        SparseTestCommon.createSparseIndex(
            client(),
            engine,
            SparseForwardIndex.PER_BLOCK,
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            8,
            0.4f,
            0.5f,
            DOC_COUNT,
            SHARDS,
            REPLICAS
        );
        assertPrimaryAndReplicaAgree();
    }

    private void assertPrimaryAndReplicaAgree() throws Exception {
        ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCS, null, 1);
        forceMerge(TEST_INDEX_NAME);
        waitForSegmentMerge(TEST_INDEX_NAME, SHARDS, REPLICAS);
        // Every copy has to be STARTED before a preference can pick one, and a replica that failed to
        // recover its engine file never gets there.
        waitForClusterHealthGreen(String.valueOf(REPLICAS + 1));
        assertEquals(SHARDS * (REPLICAS + 1), getSegmentCount(TEST_INDEX_NAME));

        Map<String, Object> primaryResults = searchWithPreference("_primary");
        Map<String, Object> replicaResults = searchWithPreference("_replica");

        assertShardsSucceeded(primaryResults, "_primary");
        assertShardsSucceeded(replicaResults, "_replica");
        assertEquals("the replica returned a different number of hits", getHitCount(primaryResults), getHitCount(replicaResults));
        assertTrue("the replica returned no hits at all", getHitCount(replicaResults) > 0);
        // Under document replication each copy builds and clusters its own segments, and native
        // clustering seeds from std::random_device, so the two copies may order the approximate top-k
        // differently. The documents they retrieve still have to be the same.
        assertEquals(
            "the replica retrieved a different set of documents",
            new HashSet<>(getDocIDs(primaryResults)),
            new HashSet<>(getDocIDs(replicaResults))
        );
    }

    private Map<String, Object> searchWithPreference(String preference) {
        NeuralSparseQueryBuilder queryBuilder = getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 2, 1.0f, DOC_COUNT, QUERY_TOKENS);
        return search(TEST_INDEX_NAME, queryBuilder, null, DOC_COUNT, Map.of("preference", preference), null);
    }

    @SuppressWarnings("unchecked")
    private void assertShardsSucceeded(Map<String, Object> searchResults, String preference) {
        Map<String, Object> shards = (Map<String, Object>) searchResults.get("_shards");
        assertNotNull(shards);
        assertEquals("shard failure with preference " + preference + ": " + shards, 0, ((Number) shards.get("failed")).intValue());
        assertEquals(
            "no shard answered with preference " + preference + ": " + shards,
            SHARDS,
            ((Number) shards.get("successful")).intValue()
        );
    }
}
