/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import org.opensearch.common.Randomness;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

public class HybridSearchRelevancyIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "neural-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "hybrid-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final int NUM_DOCS = 100;
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    private static final String QUERY_TEXT = "machine learning patterns";
    private String modelId;

    // Arrays of words to generate random meaningful content
    private static final String[] SUBJECTS = {
        "Machine learning",
        "Deep learning",
        "Neural networks",
        "Artificial intelligence",
        "Data science",
        "Natural language processing",
        "Computer vision",
        "Robotics",
        "Big data",
        "Cloud computing",
        "Edge computing",
        "Internet of Things" };

    private static final String[] VERBS = {
        "analyzes",
        "processes",
        "transforms",
        "improves",
        "optimizes",
        "enhances",
        "revolutionizes",
        "accelerates",
        "streamlines",
        "powers",
        "enables",
        "drives" };

    private static final String[] OBJECTS = {
        "data processing",
        "pattern recognition",
        "decision making",
        "business operations",
        "computational tasks",
        "system performance",
        "automation processes",
        "data analysis",
        "resource utilization",
        "technological innovation",
        "software development",
        "cloud infrastructure" };

    private static final String[] MODIFIERS = {
        "efficiently",
        "rapidly",
        "intelligently",
        "automatically",
        "significantly",
        "dramatically",
        "consistently",
        "reliably",
        "effectively",
        "seamlessly" };

    public void testSearchHitsAfterNormalization_whenIndexWithMultipleShards_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        String[] testDocuments = generateTestDocuments(NUM_DOCS);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                // ingest test documents
                for (int i = 0; i < testDocuments.length; i++) {
                    addDocument(indexName, String.valueOf(i), TEST_FIELD, testDocuments[i], null, null);
                }
                createSearchPipeline(
                    SEARCH_PIPELINE_NAME,
                    "l2",
                    "arithmetic_mean",
                    Map.of("weights", Arrays.toString(new float[] { 0.5f, 0.5f })),
                    false
                );

                // execute hybrid query and store results
                HybridQueryBuilder hybridQueryBuilder = createHybridQuery(modelId, QUERY_TEXT);
                getAndAssertQueryResults(hybridQueryBuilder, modelId, NUM_DOCS);
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                HybridQueryBuilder mixedClusterQuery = createHybridQuery(modelId, QUERY_TEXT);
                if (isFirstMixedRound()) {
                    getAndAssertQueryResults(mixedClusterQuery, modelId, NUM_DOCS);
                    String[] testDocumentsAfterMixedUpgrade = generateTestDocuments(NUM_DOCS);
                    for (int i = 0; i < testDocumentsAfterMixedUpgrade.length; i++) {
                        addDocument(indexName, String.valueOf(NUM_DOCS + i), TEST_FIELD, testDocumentsAfterMixedUpgrade[i], null, null);
                    }
                } else {
                    getAndAssertQueryResults(mixedClusterQuery, modelId, 2 * NUM_DOCS);
                }
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadAndWaitForModelToBeReady(modelId);
                    String[] testDocumentsAfterFullUpgrade = generateTestDocuments(NUM_DOCS);
                    for (int i = 0; i < testDocumentsAfterFullUpgrade.length; i++) {
                        addDocument(indexName, String.valueOf(2 * NUM_DOCS + i), TEST_FIELD, testDocumentsAfterFullUpgrade[i], null, null);
                    }
                    HybridQueryBuilder upgradedClusterQuery = createHybridQuery(modelId, QUERY_TEXT);
                    getAndAssertQueryResults(upgradedClusterQuery, modelId, 3 * NUM_DOCS);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException(String.format(Locale.ROOT, "Unexpected value: %s", getClusterType()));
        }
    }

    private String[] generateTestDocuments(int count) {
        String[] documents = new String[count];
        Random random = Randomness.get();

        for (int i = 0; i < count; i++) {
            String subject = SUBJECTS[random.nextInt(SUBJECTS.length)];
            String verb = VERBS[random.nextInt(VERBS.length)];
            String object = OBJECTS[random.nextInt(OBJECTS.length)];
            String modifier = MODIFIERS[random.nextInt(MODIFIERS.length)];

            // randomly decide whether to add a modifier (70% chance)
            boolean includeModifier = random.nextDouble() < 0.7;

            documents[i] = includeModifier
                ? String.format(Locale.ROOT, "%s %s %s %s", subject, verb, object, modifier)
                : String.format(Locale.ROOT, "%s %s %s", subject, verb, object);
        }
        return documents;
    }

    private HybridQueryBuilder createHybridQuery(String modelId, String queryText) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
            .modelId(modelId)
            .queryText(queryText)
            .k(10 * NUM_DOCS)
            .build();

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", queryText);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);

        return hybridQueryBuilder;
    }

    private void getAndAssertQueryResults(HybridQueryBuilder queryBuilder, String modelId, int queryResultSize) throws Exception {
        Map<String, Object> searchResponseAsMap = search(
            getIndexNameForTest(),
            queryBuilder,
            null,
            queryResultSize,
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME)
        );
        int hits = getHitCount(searchResponseAsMap);
        assertEquals(queryResultSize, hits);

        List<Double> normalizedScores = getNormalizationScoreList(searchResponseAsMap);
        assertQueryScores(normalizedScores, queryResultSize);
        List<String> normalizedDocIds = getNormalizationDocIdList(searchResponseAsMap);
        assertQueryDocIds(normalizedDocIds, queryResultSize);
    }

    private void assertQueryScores(List<Double> queryScores, int queryResultSize) {
        assertNotNull(queryScores);
        assertEquals(queryResultSize, queryScores.size());

        // check scores are in descending order
        for (int i = 0; i < queryScores.size() - 1; i++) {
            double currentScore = queryScores.get(i);
            double nextScore = queryScores.get(i + 1);
            assertTrue("scores not in descending order", currentScore >= nextScore);
        }
    }

    private void assertQueryDocIds(List<String> querDocIds, int queryResultSize) {
        assertNotNull(querDocIds);
        assertEquals(queryResultSize, querDocIds.size());

        // check document IDs are unique
        Set<String> uniqueDocIds = new HashSet<>();
        for (String docId : querDocIds) {
            assertTrue("duplicate document ID found", uniqueDocIds.add(docId));
        }
        assertEquals("number of unique document IDs doesn't match expected count", queryResultSize, uniqueDocIds.size());
    }
}
