/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

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
     */
    @SneakyThrows
    protected void indexTestDocuments(String indexName) {
        if (textEmbeddingModelId != null) {
            // Index documents with pipeline for neural search support
            createTextEmbeddingPipeline();

            addKnnDocWithPipeline(
                indexName,
                "1",
                "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0."
            );
            addKnnDocWithPipeline(
                indexName,
                "2",
                "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data."
            );
            addKnnDocWithPipeline(
                indexName,
                "3",
                "Natural language processing enables computers to understand, interpret, and generate human language. It combines computational linguistics with machine learning and deep learning models."
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
                    "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0."
                )
            );
            addKnnDoc(
                indexName,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data."
                )
            );
            addKnnDoc(
                indexName,
                "3",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Natural language processing enables computers to understand, interpret, and generate human language. It combines computational linguistics with machine learning and deep learning models."
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

}
