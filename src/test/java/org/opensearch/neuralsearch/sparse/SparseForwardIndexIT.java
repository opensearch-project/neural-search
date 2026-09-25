/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration tests for the {@code forward_index} mapping parameter, which picks the layout the
 * native engine stores the per-document vectors in. Native only: the Lucene engine has a single
 * forward index layout and rejects any non-default value.
 */
public class SparseForwardIndexIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-forward-index";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    /** Documents the exact-match filter keeps, which stays below every {@code k} used below. */
    private static final int FILTERED_DOC_COUNT = 3;

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

    /**
     * A filter no larger than {@code k} has to leave the block-budget path and match exactly, the way
     * the shared layout and the Lucene engine already do. per_block reaches that decision inside
     * nsparse rather than through {@code SparseQueryWeight#selectScorer}, so it is the one layout
     * where a filtered query can silently come back empty.
     *
     * <p>The corpus has to be large enough that the budget is the binding constraint: with a few
     * blocks in total, k' covers them all and even a broken build finds the filtered documents by
     * accident. 500 documents at cluster_ratio 0.1 give ~50 blocks per posting against a budget of
     * 20, and the 3 documents this filter keeps are then very unlikely to sit in the top blocks.
     */
    public void testSearchDocuments_withPerBlockForwardIndexAndFilterSmallerThanK() throws Exception {
        int docCount = 500;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, SparseForwardIndex.PER_BLOCK, docCount, 0.4f, 0.1f, docCount);

        List<String> texts = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            texts.add(i < FILTERED_DOC_COUNT ? "apple" : "tree");
        }
        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            prepareIngestDocuments(docCount),
            texts
        );

        // 3 documents pass, well under k = 10.
        BoolQueryBuilder filter = new BoolQueryBuilder().must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"));
        Map<String, Float> queryTokens = Map.of("1000", 1.0f);
        Map<String, Object> searchResults = search(
            TEST_INDEX_NAME,
            getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 1, 1.0f, 10, queryTokens, filter),
            10
        );
        assertNotNull(searchResults);
        assertEquals("every filtered document has to be scored on the exact-match path", FILTERED_DOC_COUNT, getHitCount(searchResults));
        // Weights are random, so the order among the three is not fixed, but the set is.
        assertEquals(Set.of("1", "2", "3"), new HashSet<>(getDocIDs(searchResults)));

        // The exact-match path neither prunes nor reads block summaries, so heap_factor cannot move
        // it. A result that changes with heap_factor means the query stayed on the budget path.
        Map<String, Object> highHeapFactorResults = search(
            TEST_INDEX_NAME,
            getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 1, 20.0f, 10, queryTokens, filter),
            10
        );
        assertEquals(getDocIDs(searchResults), getDocIDs(highHeapFactorResults));
        assertEquals(getNormalizationScoreList(searchResults), getNormalizationScoreList(highHeapFactorResults));
    }

    /**
     * per_block does not prune with a relative threshold: it scores the globally best k' block
     * summaries and reads exactly those, so {@code NativeIndexScorer} converts heap_factor into k'
     * through {@code DiskSeismicKPrime} (20 blocks at 1.0, 120 at 2.0). k' bounds how many documents
     * can be scored at all, so raising heap_factor has to widen the result set on a corpus with more
     * blocks than the budget. A flat hit count across the sweep means the conversion never reached
     * nsparse - the regression #1997 fixed, which unit tests on the arithmetic alone cannot see.
     */
    public void testHeapFactorWidensTheBlockBudget() throws Exception {
        int docCount = 500;
        // cluster_ratio 0.1 over a posting list holding all 500 documents gives ~50 blocks per
        // posting, more than the 20 the default heap_factor budgets for.
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, SparseForwardIndex.PER_BLOCK, docCount, 0.4f, 0.1f, docCount);
        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            prepareIngestDocuments(docCount)
        );

        // A single query token keeps the candidate blocks to one posting list, so the budget stays
        // the binding constraint. k is the whole corpus so the hit count reports documents scored.
        Map<String, Float> queryTokens = Map.of("1000", 1.0f);
        List<Float> heapFactors = List.of(1.0f, 1.3f, 2.0f);
        List<Integer> hitCounts = new ArrayList<>();
        for (float heapFactor : heapFactors) {
            Map<String, Object> results = search(
                TEST_INDEX_NAME,
                getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 1, heapFactor, docCount, queryTokens),
                docCount
            );
            assertNotNull(results);
            hitCounts.add(getHitCount(results));
        }

        for (int i = 1; i < hitCounts.size(); ++i) {
            assertTrue(
                "raising heap_factor shrank the result set: " + heapFactors + " -> " + hitCounts,
                hitCounts.get(i) >= hitCounts.get(i - 1)
            );
        }
        assertTrue(
            "heap_factor never reached k': the same documents came back at 1.0 and 2.0, " + heapFactors + " -> " + hitCounts,
            hitCounts.get(hitCounts.size() - 1) > hitCounts.get(0)
        );
    }
}
