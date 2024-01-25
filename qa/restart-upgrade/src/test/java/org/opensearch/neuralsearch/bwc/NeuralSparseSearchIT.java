/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.TestUtils;
import static org.opensearch.neuralsearch.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.SPARSE_ENCODING_PROCESSOR;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

public class NeuralSparseSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline-sparse";
    private static final String TEST_SPARSE_ENCODING_FIELD = "passage_embedding";
    private static final String TEST_TEXT_FIELD = "passage_text";
    private static final String TEXT_1 = "Hello world a b";
    private static final String TEXT_2 = "Hello planet";
    private static final List<String> TEST_TOKENS_1 = List.of("hello", "world", "a", "b", "c");
    private static final List<String> TEST_TOKENS_2 = List.of("hello", "planet", "a", "b", "c");
    private final Map<String, Float> testRankFeaturesDoc1 = TestUtils.createRandomTokenWeightMap(TEST_TOKENS_1);
    private final Map<String, Float> testRankFeaturesDoc2 = TestUtils.createRandomTokenWeightMap(TEST_TOKENS_2);

    // Test restart-upgrade test sparse embedding processor
    // Create Sparse Encoding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testSparseEncodingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadSparseEncodingModel();
            loadModel(modelId);
            createPipelineForSparseEncodingProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                PIPELINE_NAME
            );

            addSparseEncodingDoc(
                getIndexNameForTest(),
                "0",
                List.of(TEST_SPARSE_ENCODING_FIELD),
                List.of(testRankFeaturesDoc1),
                List.of(TEST_TEXT_FIELD),
                List.of(TEXT_1)
            );
        } else {
            String modelId = null;
            try {
                modelId = TestUtils.getModelId(getIngestionPipeline(PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
                loadModel(modelId);
                addSparseEncodingDoc(
                    getIndexNameForTest(),
                    "1",
                    List.of(TEST_SPARSE_ENCODING_FIELD),
                    List.of(testRankFeaturesDoc2),
                    List.of(TEST_TEXT_FIELD),
                    List.of(TEXT_2)
                );
                validateTestIndex(modelId);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
            }
        }

    }

    private void validateTestIndex(final String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_SPARSE_ENCODING_FIELD)
            .queryText(TEXT_1)
            .modelId(modelId);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD, TEXT_1);
        boolQueryBuilder.should(sparseEncodingQueryBuilder).should(matchQueryBuilder);
        Map<String, Object> response = search(getIndexNameForTest(), boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(response);

        assertEquals("0", firstInnerHit.get("_id"));
        float minExpectedScore = computeExpectedScore(modelId, testRankFeaturesDoc1, TEXT_1);
        assertTrue(minExpectedScore < objectToFloat(firstInnerHit.get("_score")));
    }
}
