/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.TestUtils;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class SemanticFieldIT extends AbstractRestartUpgradeRestTestCase {
    private static final String TEST_SEMANTIC_TEXT_FIELD = "test_field";
    private static final String TEST_SEMANTIC_TEXT_FIELD_PATH = "mappings.properties.test_field";
    private static final String SEMANTIC_INFO_FIELD = "semantic_info";
    private static final String SEMANTIC_EMBEDDING_FIELD = "embedding";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");
    private final Map<String, Float> testRankFeaturesDoc = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

    // Test restart-upgrade test semantic field processor
    // Create Index with Semantic Field and add document
    // Validate document with generated embeddings in restart-upgrade scenario
    public void testSemanticFieldProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String modelId = null;
        if (isRunningAgainstOldCluster()) {
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
        } else {
            try {
                loadAndWaitForModelToBeReady(
                    getModelId(getIndexMapping(getIndexNameForTest()), getIndexNameForTest(), TEST_SEMANTIC_TEXT_FIELD_PATH)
                );
                addSemanticDoc(
                    getIndexNameForTest(),
                    "1",
                    String.format(LOCALE, "%s_%s", TEST_SEMANTIC_TEXT_FIELD, SEMANTIC_INFO_FIELD),
                    List.of(SEMANTIC_EMBEDDING_FIELD),
                    List.of(testRankFeaturesDoc)
                );
                validateTestIndex();
            } finally {
                wipeOfTestResources(getIndexNameForTest(), null, modelId, null);
            }
        }

    }

    private void validateTestIndex() {
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
