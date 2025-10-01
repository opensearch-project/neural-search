/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.util.AggregationsTestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;

/**
 * Base class for Semantic Highlighting integration tests.
 * Provides common setup, teardown, and utility methods for both local and remote model testing.
 */
@Log4j2
public abstract class BaseSemanticHighlightingIT extends BaseNeuralSearchIT {

    protected static final String TEST_FIELD = "content";
    protected static final String TEST_KNN_VECTOR_FIELD = "content_embedding";
    protected static final int TEST_DIMENSION = 768;
    protected static final String TEXT_EMBEDDING_PIPELINE = "test-text-embedding-pipeline";

    protected String textEmbeddingModelId;  // For neural queries

    @Before
    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        updateMLCommonsSettings();
    }

    @After
    @SneakyThrows
    @Override
    public void tearDown() {
        // Delete text embedding pipeline
        try {
            Request request = new Request("DELETE", "/_ingest/pipeline/" + TEXT_EMBEDDING_PIPELINE);
            client().performRequest(request);
        } catch (Exception e) {
            log.debug("Failed to delete pipeline: {}", e.getMessage());
        }

        // Cleanup text embedding model
        try {
            if (textEmbeddingModelId != null) {
                deleteModel(textEmbeddingModelId);
            }
        } catch (Exception e) {
            log.debug("Failed to delete text embedding model: {}", e.getMessage());
        }

        super.tearDown();
    }

    /**
     * Update ML Commons settings for semantic highlighting tests
     */
    @SneakyThrows
    protected void updateMLCommonsSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        updateClusterSettings("plugins.ml_commons.connector.private_ip_enabled", true);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
        updateClusterSettings(
            "plugins.ml_commons.trusted_connector_endpoints_regex",
            List.of(
                "^https://runtime\\.sagemaker\\..*[a-z0-9-]\\.amazonaws\\.com/.*$",
                "^http://localhost:.*",
                "^http://127\\.0\\.0\\.1:.*",
                "^http://torchserve:.*"
            )
        );
    }

    /**
     * Prepare a KNN index for semantic highlighting tests
     */
    @SneakyThrows
    protected void prepareHighlightingIndex(String indexName) {
        prepareKnnIndex(indexName, Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)));
    }

    /**
     * Create text embedding pipeline for indexing documents
     */
    @SneakyThrows
    protected void createTextEmbeddingPipeline() {
        if (textEmbeddingModelId == null) {
            return;
        }

        XContentBuilder pipelineBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("processors")
            .startObject()
            .startObject("text_embedding")
            .field("model_id", textEmbeddingModelId)
            .startObject("field_map")
            .field(TEST_FIELD, TEST_KNN_VECTOR_FIELD)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        Request createPipelineRequest = new Request("PUT", "/_ingest/pipeline/" + TEXT_EMBEDDING_PIPELINE);
        createPipelineRequest.setJsonEntity(pipelineBuilder.toString());
        Response pipelineResponse = client().performRequest(createPipelineRequest);
        assertEquals(200, pipelineResponse.getStatusLine().getStatusCode());
    }

    /**
     * Index a document with the text embedding pipeline
     */
    @SneakyThrows
    protected void addKnnDocWithPipeline(String indexName, String docId, String fieldName, String content, String pipeline) {
        Request request = new Request("PUT", "/" + indexName + "/_doc/" + docId + "?pipeline=" + pipeline + "&refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field(fieldName, content).endObject();
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    /**
     * Index a document with the text embedding pipeline using the default pipeline name
     */
    @SneakyThrows
    protected void addKnnDocWithPipeline(String indexName, String docId, String content) {
        addKnnDocWithPipeline(indexName, docId, TEST_FIELD, content, TEXT_EMBEDDING_PIPELINE);
    }

    /**
     * Index test documents for semantic highlighting tests
     * Uses medical research documents about neurodegenerative diseases to demonstrate semantic highlighting
     */
    @SneakyThrows
    protected void indexTestDocuments(String indexName) {
        if (textEmbeddingModelId != null) {
            // Index documents with pipeline for neural search support
            createTextEmbeddingPipeline();

            addKnnDocWithPipeline(
                indexName,
                "1",
                "Treatments for neurodegenerative diseases like Parkinson disease include various therapeutic approaches. Parkinson disease is a progressive disorder with multiple treatment options including cholinesterase inhibitors and clinical trials. Recent advances in treatments for neurodegenerative diseases have shown promising results in managing symptoms and slowing disease progression."
            );
            addKnnDocWithPipeline(
                indexName,
                "2",
                "Treatments for neurodegenerative diseases like ALS are advancing rapidly. ALS disease requires specialized therapeutic interventions and ongoing clinical trials. Innovative treatments for neurodegenerative diseases focus on disease-modifying therapies and supportive care to improve patient outcomes."
            );
            addKnnDocWithPipeline(
                indexName,
                "3",
                "Treatments for neurodegenerative diseases such as Alzheimer disease involve multiple strategies. Alzheimer disease treatments include gene therapy and novel pharmaceutical interventions. Research into treatments for neurodegenerative diseases continues to identify new therapeutic targets."
            );
            addKnnDocWithPipeline(
                indexName,
                "4",
                "Treatments for neurodegenerative diseases like Huntington disease are evolving. Huntington disease management includes cognitive rehabilitation and experimental therapies. New treatments for neurodegenerative diseases offer hope through precision medicine approaches."
            );
            addKnnDocWithPipeline(
                indexName,
                "5",
                "Treatments for neurodegenerative diseases including Lewy Body Dementia show promise. Lewy Body Dementia requires comprehensive treatment plans with stem cell therapy trials. Emerging treatments for neurodegenerative diseases target specific molecular pathways to slow progression."
            );
        } else {
            // Index documents without pipeline for basic match query tests
            addKnnDoc(
                indexName,
                "1",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Treatments for neurodegenerative diseases like Parkinson disease include various therapeutic approaches. Parkinson disease is a progressive disorder with multiple treatment options including cholinesterase inhibitors and clinical trials. Recent advances in treatments for neurodegenerative diseases have shown promising results in managing symptoms and slowing disease progression."
                )
            );
            addKnnDoc(
                indexName,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Treatments for neurodegenerative diseases like ALS are advancing rapidly. ALS disease requires specialized therapeutic interventions and ongoing clinical trials. Innovative treatments for neurodegenerative diseases focus on disease-modifying therapies and supportive care to improve patient outcomes."
                )
            );
            addKnnDoc(
                indexName,
                "3",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Treatments for neurodegenerative diseases such as Alzheimer disease involve multiple strategies. Alzheimer disease treatments include gene therapy and novel pharmaceutical interventions. Research into treatments for neurodegenerative diseases continues to identify new therapeutic targets."
                )
            );
            addKnnDoc(
                indexName,
                "4",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Treatments for neurodegenerative diseases like Huntington disease are evolving. Huntington disease management includes cognitive rehabilitation and experimental therapies. New treatments for neurodegenerative diseases offer hope through precision medicine approaches."
                )
            );
            addKnnDoc(
                indexName,
                "5",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Treatments for neurodegenerative diseases including Lewy Body Dementia show promise. Lewy Body Dementia requires comprehensive treatment plans with stem cell therapy trials. Emerging treatments for neurodegenerative diseases target specific molecular pathways to slow progression."
                )
            );
        }
    }

    @SneakyThrows
    protected String prepareSentenceHighlightingModel() {
        String requestBody = Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("highlight/LocalQuestionAnsweringModel.json")).toURI())
        );
        String modelId = registerModelGroupAndUploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    /**
     * Assert semantic highlighting with default tags (<em></em>)
     * This is a semantic highlighting-specific assertion method
     */
    protected void assertSemanticHighlighting(Map<String, Object> responseMap, String fieldName, String expectedHighlight) {
        assertSemanticHighlighting(responseMap, fieldName, expectedHighlight, "<em>", "</em>");
    }

    /**
     * Assert semantic highlighting with custom tags
     * Verifies:
     * 1. Response structure is correct
     * 2. Highlight tags are present in the fragments
     * 3. Expected text appears in the highlighted content
     * 4. Checks all hits, not just the first one
     */
    protected void assertSemanticHighlighting(
        Map<String, Object> responseMap,
        String fieldName,
        String expectedHighlight,
        String preTag,
        String postTag
    ) {
        // 1. Verify response structure
        assertNotNull("Response should not be null", responseMap);
        List<Map<String, Object>> hits = AggregationsTestUtils.getNestedHits(responseMap);
        assertNotNull("Response should contain hits", hits);
        assertFalse("Should have at least one hit", hits.isEmpty());

        // 2. Check each hit for highlights (not just the first one)
        boolean foundHighlight = false;
        StringBuilder allFragments = new StringBuilder();

        for (Map<String, Object> hit : hits) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(fieldName)) {
                List<String> fragments = (List<String>) highlight.get(fieldName);
                assertNotNull("Highlight fragments should not be null", fragments);
                assertFalse("Highlight fragments should not be empty", fragments.isEmpty());

                // 3. Verify highlight structure
                for (String fragment : fragments) {
                    allFragments.append(fragment).append(" ");

                    // Check that highlight tags are present
                    assertTrue("Fragment should contain opening tag '" + preTag + "' in: " + fragment, fragment.contains(preTag));
                    assertTrue("Fragment should contain closing tag '" + postTag + "' in: " + fragment, fragment.contains(postTag));

                    // 4. Verify the expected text is somewhere in the fragment
                    String plainText = fragment.replaceAll("<[^>]*>", "");
                    if (plainText.toLowerCase(Locale.ROOT).contains(expectedHighlight.toLowerCase(Locale.ROOT))) {
                        foundHighlight = true;
                    }
                }
            }
        }

        assertTrue(
            "Should find expected text '" + expectedHighlight + "' in at least one highlighted fragment. Fragments: " + allFragments,
            foundHighlight
        );
    }

}
