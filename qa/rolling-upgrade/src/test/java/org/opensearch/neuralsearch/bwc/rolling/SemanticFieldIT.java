/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.TestUtils;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class SemanticFieldIT extends AbstractRollingUpgradeTestCase {
    private static final String TEST_SEMANTIC_TEXT_FIELD = "test_field";
    private static final String TEST_SEMANTIC_TEXT_FIELD_PATH = "mappings.properties.test_field";
    private static final String SEMANTIC_INFO_FIELD = "semantic_info";
    private static final String SEMANTIC_EMBEDDING_FIELD = "embedding";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static String modelId = "";
    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");
    private final Map<String, Float> testRankFeaturesDoc = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

    // Test rolling-upgrade test semantic field processor
    // Create Index with Semantic Field and add document
    // Validate document with generated embeddings in rolling-upgrade scenario
    public void testSemanticFieldProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadSparseEncodingModel();
                prepareSemanticIndex(
                    getIndexNameForTest(),
                    Collections.singletonList(new SemanticFieldConfig(TEST_SEMANTIC_TEXT_FIELD)),
                    modelId,
                    null
                );
                addSemanticDoc(
                    getIndexNameForTest(),
                    "0",
                    String.format(LOCALE, "%s_%s", TEST_SEMANTIC_TEXT_FIELD, SEMANTIC_INFO_FIELD),
                    List.of(SEMANTIC_EMBEDDING_FIELD),
                    List.of(testRankFeaturesDoc)
                );
                break;
            case MIXED:
                modelId = getModelId(getIndexMapping(getIndexNameForTest()), getIndexNameForTest(), TEST_SEMANTIC_TEXT_FIELD_PATH);
                loadAndWaitForModelToBeReady(modelId);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndex(totalDocsCountMixed);
                    addSemanticDoc(
                        getIndexNameForTest(),
                        "1",
                        String.format(LOCALE, "%s_%s", TEST_SEMANTIC_TEXT_FIELD, SEMANTIC_INFO_FIELD),
                        List.of(SEMANTIC_EMBEDDING_FIELD),
                        List.of(testRankFeaturesDoc)
                    );
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndex(totalDocsCountMixed);
                }
                break;
            case UPGRADED:
                try {
                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    modelId = getModelId(getIndexMapping(getIndexNameForTest()), getIndexNameForTest(), TEST_SEMANTIC_TEXT_FIELD_PATH);
                    loadAndWaitForModelToBeReady(modelId);
                    addSemanticDoc(
                        getIndexNameForTest(),
                        "2",
                        String.format(LOCALE, "%s_%s", TEST_SEMANTIC_TEXT_FIELD, SEMANTIC_INFO_FIELD),
                        List.of(SEMANTIC_EMBEDDING_FIELD),
                        List.of(testRankFeaturesDoc)
                    );
                    validateTestIndex(totalDocsCountUpgraded);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), null, modelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateTestIndex(final int numberOfDocs) {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_SEMANTIC_TEXT_FIELD)
            .queryText(TEST_QUERY_TEXT)
            .boost(2.0f)
            .build();

        Map<String, Object> searchResponseAsMap = search(getIndexNameForTest(), neuralQueryBuilder, 1);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("0", firstInnerHit.get("_id"));
    }
}
