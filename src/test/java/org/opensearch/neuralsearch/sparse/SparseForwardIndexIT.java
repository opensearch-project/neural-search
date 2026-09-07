/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for the {@code forward_index} mapping parameter, which picks the layout the
 * native engine stores the per-document vectors in. Native only: the Lucene engine has a single
 * forward index layout and rejects any non-default value.
 */
public class SparseForwardIndexIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-forward-index";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return nativeEngineOnly();
    }

    public SparseForwardIndexIT(SparseEngine engine) {
        super(engine);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * A per_block index is built and searched through a different nsparse index type
     * (disk_seismic_sq), reached over the same write/mmap-load path and quantized over the same
     * range, so it has to return the same hits as the default shared layout.
     */
    public void testSearchDocuments_withPerBlockForwardIndex() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, SparseForwardIndex.PER_BLOCK, 4, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        NeuralSparseQueryBuilder queryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, queryBuilder, 10);
        assertNotNull(searchResults);
        // n_postings = 4 prunes each posting list to its 4 highest-weight documents.
        assertEquals(4, getHitCount(searchResults));
        assertEquals(List.of("8", "7", "6", "5"), getDocIDs(searchResults));
    }
}
