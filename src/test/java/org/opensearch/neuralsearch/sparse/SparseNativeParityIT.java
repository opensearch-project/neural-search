/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Cross-arm parity tests for the native engine.
 *
 * <p>Every other sparse suite is parameterized over {@link SparseEngine} and asserts each engine
 * against its own expectations, so a systematic scoring error on the native side - a dropped token,
 * a rescaled weight, an off-by-one document - passes them all. These tests build a second index on
 * a reference implementation and require the two to agree, which is the only assertion in the suite
 * that can tell "native is correct" apart from "native is self-consistently wrong".
 *
 * <p>Only the paths where agreement is exact are compared here. The approximate seismic path prunes
 * with clustering that seeds from {@code std::random_device} on the native side, so its result set
 * is not reproducible run to run and belongs in the manual recall sanity tests instead.
 */
public class SparseNativeParityIT extends SparseBaseIT {

    private static final String NATIVE_INDEX = "test-native-parity";
    private static final String LUCENE_INDEX = "test-lucene-parity";
    private static final String RANK_FEATURES_INDEX = "test-rank-features-parity";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";

    /**
     * Doc weights are multiples of 1/8. {@code FeatureField} stores a weight as
     * {@code floatToIntBits(w) >>> 15}, keeping 8 mantissa bits, so a weight needing more than that
     * comes back from the rank_features arm truncated by up to 0.4% - a disagreement with the
     * unquantized native arm that is not a bug. These values round-trip exactly, which keeps the
     * comparison below a strict one.
     */
    private static final List<Map<String, Float>> DOCS = List.of(
        Map.of("1000", 0.125f, "2000", 0.25f, "3000", 0.5f),
        Map.of("1000", 0.25f, "2000", 0.5f, "3000", 0.125f),
        Map.of("1000", 0.375f, "2000", 0.75f, "3000", 0.25f),
        Map.of("1000", 0.5f, "2000", 1.0f, "3000", 0.375f),
        Map.of("1000", 0.625f, "2000", 1.25f, "3000", 0.5f),
        Map.of("1000", 0.75f, "2000", 1.5f, "3000", 0.625f),
        Map.of("1000", 0.875f, "2000", 1.75f, "3000", 0.75f),
        Map.of("1000", 1.0f, "2000", 2.0f, "3000", 0.875f)
    );

    /** Half the corpus matches "apple", which gives a filter of cardinality 4. */
    private static final List<String> TEXTS = List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree");

    private static final Map<String, Float> QUERY_TOKENS = Map.of("1000", 2.0f, "2000", 1.5f, "3000", 0.5f);

    /** Above the 8-doc corpus, so no segment is clustered and both engines run the exact path. */
    private static final int THRESHOLD_ABOVE_CORPUS = 1000;

    /** At or below the 8-doc corpus, so the whole field is clustered. */
    private static final int THRESHOLD_AT_CORPUS = 8;

    private static final int K = 10;

    /** Both arms compute a float dot product; only summation order can differ. */
    private static final double EXACT_SCORE_DELTA = 1e-5;

    /**
     * The forward index a seismic segment scores from holds 8-bit codes, and the native and Lucene
     * quantizers do not derive their scale the same way (see {@link #QUANTIZATION_IS_LUCENE_ONLY}),
     * so scores on that path are only compared to the unquantized dot product loosely. Tight enough
     * to catch a wrong document, a dropped token or a rescaled weight.
     */
    private static final double QUANTIZED_SCORE_TOLERANCE = 0.05;

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return nativeEngineOnly();
    }

    public SparseNativeParityIT(SparseEngine engine) {
        super(engine);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * The inverted path is unquantized on both arms - nsparse holds raw float32 postings and
     * rank_features holds a losslessly encoded weight for the values used here - so the native
     * scores have to equal the rank_features scores, and both have to equal the dot product this
     * test computes itself. Any divergence here is a real bug.
     */
    public void testInvertedIndexPathMatchesRankFeaturesBaseline() throws Exception {
        createSparseIndex(NATIVE_INDEX, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, THRESHOLD_ABOVE_CORPUS);
        ingestDocumentsAndForceMergeForSingleShard(NATIVE_INDEX, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCS, TEXTS);

        prepareSparseEncodingIndex(RANK_FEATURES_INDEX, List.of(TEST_SPARSE_FIELD_NAME), 1);
        ingestDocuments(RANK_FEATURES_INDEX, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCS, TEXTS, 1);

        Map<String, Object> nativeResults = search(
            NATIVE_INDEX,
            getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, QUERY_TOKENS.size(), 1.0f, K, QUERY_TOKENS),
            K
        );
        // The rank_features field has no method_parameters to accept, so the baseline is queried
        // with the plain neural_sparse body.
        Map<String, Object> baselineResults = search(RANK_FEATURES_INDEX, rankFeaturesQuery(), K);

        List<String> expectedIds = expectedIdsByScore(null);
        assertEquals(expectedIds, getDocIDs(nativeResults));
        assertEquals("the rank_features baseline itself disagrees with the dot product", expectedIds, getDocIDs(baselineResults));

        List<Double> nativeScores = getNormalizationScoreList(nativeResults);
        List<Double> baselineScores = getNormalizationScoreList(baselineResults);
        assertEquals(expectedIds.size(), nativeScores.size());
        for (int i = 0; i < expectedIds.size(); ++i) {
            double expected = expectedScore(expectedIds.get(i));
            assertEquals(
                "score mismatch on the rank_features baseline for _id " + expectedIds.get(i),
                expected,
                baselineScores.get(i),
                EXACT_SCORE_DELTA
            );
            assertEquals(
                "native score diverges from the baseline for _id " + expectedIds.get(i),
                expected,
                nativeScores.get(i),
                EXACT_SCORE_DELTA
            );
        }
    }

    /**
     * A filter no larger than {@code k} sends both engines down the exact-match path, which does not
     * prune, so the two arms have to return the same documents in the same order. Scores are read
     * from a quantized forward index on this path, so they are checked against the dot product
     * within {@link #QUANTIZED_SCORE_TOLERANCE} rather than against each other.
     */
    public void testExactMatchFilterPathMatchesLuceneEngine() throws Exception {
        createSparseIndex(NATIVE_INDEX, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, THRESHOLD_AT_CORPUS);
        ingestDocumentsAndForceMergeForSingleShard(NATIVE_INDEX, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, DOCS, TEXTS);

        // Built through SparseTestCommon rather than the inherited helper: this suite runs on the
        // native engine, and the point of the test is to compare it against the other one.
        SparseTestCommon.createSparseIndex(
            client(),
            SparseEngine.LUCENE,
            LUCENE_INDEX,
            TEST_SPARSE_FIELD_NAME,
            8,
            0.4f,
            0.5f,
            THRESHOLD_AT_CORPUS
        );
        SparseTestCommon.ingestDocumentsAndForceMergeForSingleShard(
            client(),
            LUCENE_INDEX,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            DOCS,
            TEXTS
        );

        List<String> expectedIds = expectedIdsByScore("apple");
        assertEquals("filter has to stay at or below k to reach the exact-match path", 4, expectedIds.size());

        for (String index : List.of(NATIVE_INDEX, LUCENE_INDEX)) {
            Map<String, Object> results = search(
                index,
                getNeuralSparseQueryBuilder(
                    TEST_SPARSE_FIELD_NAME,
                    QUERY_TOKENS.size(),
                    1.0f,
                    K,
                    QUERY_TOKENS,
                    new BoolQueryBuilder().must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "apple"))
                ),
                K
            );
            assertEquals("the exact-match path dropped a filtered document on " + index, expectedIds.size(), getHitCount(results));
            assertEquals("exact-match ordering differs on " + index, expectedIds, getDocIDs(results));

            List<Double> scores = getNormalizationScoreList(results);
            for (int i = 0; i < expectedIds.size(); ++i) {
                double expected = expectedScore(expectedIds.get(i));
                assertEquals(
                    "score on "
                        + index
                        + " for _id "
                        + expectedIds.get(i)
                        + " is not within "
                        + QUANTIZED_SCORE_TOLERANCE
                        + " of the dot product",
                    expected,
                    scores.get(i),
                    expected * QUANTIZED_SCORE_TOLERANCE
                );
            }
        }
    }

    /** Plain neural_sparse body for the rank_features baseline. */
    private NeuralSparseQueryBuilder rankFeaturesQuery() {
        return new NeuralSparseQueryBuilder().fieldName(TEST_SPARSE_FIELD_NAME).queryTokensMapSupplier(() -> QUERY_TOKENS);
    }

    /**
     * Dot product of the query against the document ingested under {@code docId}. Weights are read
     * back through the {@code FeatureField} encoding so the same expectation holds for both arms;
     * for the values in {@link #DOCS} that encoding is a no-op.
     */
    private double expectedScore(String docId) {
        return computeExpectedScore(DOCS.get(Integer.parseInt(docId) - 1), QUERY_TOKENS);
    }

    /**
     * Document ids ordered by descending score, restricted to the documents whose text is
     * {@code matchingText} when one is given. Ids are assigned by the ingest helper starting at 1.
     */
    private List<String> expectedIdsByScore(String matchingText) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < DOCS.size(); ++i) {
            if (matchingText == null || matchingText.equals(TEXTS.get(i))) {
                ids.add(String.valueOf(i + 1));
            }
        }
        ids.sort(Comparator.comparingDouble(this::expectedScore).reversed());
        return ids;
    }
}
