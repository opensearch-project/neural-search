/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * Integration tests for Symmetric Text Embedding using remote models via TorchServe.
 * Tests symmetric text embedding functionality with the symmetric_text_embedding handler.
 */
@Log4j2
public class SymmetricRemoteModelIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "symmetric_remote_index";
    private static final String PIPELINE_NAME = "symmetric_remote_pipeline";

    // Test document fields
    protected static final String QUERY_TEXT = "hello";
    protected static final String TEXT_FIELD_NAME = "passage_text";
    protected static final String EMBEDDING_FIELD_NAME = "passage_embedding";
    protected static final String NESTED_FIELD_NAME = "nested_passages";
    protected static final String LEVEL_2_FIELD = "level_2";
    protected static final String LEVEL_3_FIELD_TEXT = "level_3_text";
    protected static final String LEVEL_3_FIELD_CONTAINER = "level_3_container";
    protected static final String LEVEL_3_FIELD_EMBEDDING = "level_3_embedding";

    // Load test documents
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String INGEST_DOC3 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc3.json").toURI()));
    private final String INGEST_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc4.json").toURI()));
    private final String UPDATE_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/update_doc1.json").toURI()));
    private final String UPDATE_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/update_doc2.json").toURI()));
    private final String BULK_ITEM_TEMPLATE = Files.readString(
        Path.of(classLoader.getResource("processor/bulk_item_template.json").toURI())
    );

    private String remoteModelId;
    private String connectorId;
    private boolean isTorchServeAvailable = false;
    private static final int EMBEDDING_DIMENSION = 128; // Using tiny model with 128 dimensions

    public SymmetricRemoteModelIT() throws IOException, URISyntaxException {}

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();

        // Configure ML Commons to trust localhost endpoints for remote models
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

        // Check for TorchServe endpoint availability
        String torchServeEndpoint = System.getenv("TORCHSERVE_ENDPOINT");
        if (torchServeEndpoint == null) {
            torchServeEndpoint = System.getProperty("tests.torchserve.endpoint");
        }

        if (torchServeEndpoint != null && !torchServeEndpoint.isEmpty()) {
            // Check if TorchServe is available
            isTorchServeAvailable = RemoteModelTestUtils.isRemoteEndpointAvailable(torchServeEndpoint);
            if (isTorchServeAvailable) {
                log.info("TorchServe endpoint available at: {}", torchServeEndpoint);

                try {
                    // Create remote connector
                    connectorId = createRemoteModelConnector(torchServeEndpoint);
                    log.info("Created connector with ID: {}", connectorId);

                    // Deploy the symmetric text embedding model
                    remoteModelId = deployRemoteModel(connectorId, "symmetric-text-embedding-remote");
                    log.info("Deployed remote text embedding model with ID: {}", remoteModelId);

                } catch (Exception e) {
                    log.error("Failed to set up remote model: ", e);
                    isTorchServeAvailable = false;
                }
            } else {
                log.info("TorchServe not available at {}, tests will be skipped", torchServeEndpoint);
            }
        } else {
            log.info("TorchServe endpoint not configured, tests will be skipped");
        }
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();

        // Clean up indexes
        try {
            deleteIndex(INDEX_NAME);
        } catch (Exception e) {
            log.debug("Index cleanup failed: {}", e.getMessage());
        }

        // Clean up pipelines
        try {
            deleteIngestPipeline(PIPELINE_NAME);
        } catch (Exception e) {
            log.debug("Pipeline cleanup failed: {}", e.getMessage());
        }

        // Clean up remote model resources
        if (remoteModelId != null || connectorId != null) {
            cleanupRemoteModelResources(connectorId, remoteModelId);
        }
    }

    /**
     * Test basic document ingestion with remote symmetric model
     * Verifies that ML Commons connector and model work correctly for single document
     */
    @SneakyThrows
    public void testBasicRemoteModelIngestion() {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create text embedding pipeline with remote model
        String pipelineConfig = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"description\": \"Text embedding pipeline with remote symmetric model\",\n"
                + "  \"processors\": [\n"
                + "    {\n"
                + "      \"text_embedding\": {\n"
                + "        \"model_id\": \"%s\",\n"
                + "        \"field_map\": {\n"
                + "          \"%s\": \"%s\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            remoteModelId,
            TEXT_FIELD_NAME,
            EMBEDDING_FIELD_NAME
        );

        // Create pipeline using the raw JSON configuration
        createPipelineProcessor(pipelineConfig, PIPELINE_NAME, remoteModelId, null);

        // Create index with appropriate mapping for 128-dim embeddings
        String indexMapping = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"settings\": {\n"
                + "    \"index.knn\": true,\n"
                + "    \"default_pipeline\": \"%s\"\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"knn_vector\",\n"
                + "        \"dimension\": %d,\n"
                + "        \"method\": {\n"
                + "          \"name\": \"hnsw\",\n"
                + "          \"engine\": \"lucene\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"text\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            PIPELINE_NAME,
            EMBEDDING_FIELD_NAME,
            EMBEDDING_DIMENSION,
            TEXT_FIELD_NAME
        );

        createIndexWithConfiguration(INDEX_NAME, indexMapping, PIPELINE_NAME);

        // Ingest a document through ML Commons remote model
        String testDoc = String.format(
            Locale.ROOT,
            "{\n" + "  \"%s\": \"OpenSearch is a distributed search and analytics engine\"\n" + "}",
            TEXT_FIELD_NAME
        );

        ingestDocument(INDEX_NAME, testDoc, "1");
        assertEquals(1, getDocCount(INDEX_NAME));

        // Verify the embedding was created by remote model
        Map<String, Object> doc = getDocById(INDEX_NAME, "1");
        assertNotNull(doc);

        Map<String, Object> source = (Map<String, Object>) doc.get("_source");
        assertNotNull(source);

        List<Number> embedding = (List<Number>) source.get(EMBEDDING_FIELD_NAME);
        assertNotNull("Remote model should create embedding", embedding);
        assertEquals("Remote model embedding should have correct dimensions", EMBEDDING_DIMENSION, embedding.size());

        // Verify the embedding values are valid (not all zeros)
        boolean hasNonZeroValues = embedding.stream().anyMatch(value -> value.doubleValue() != 0.0);
        assertTrue("Embedding should contain non-zero values", hasNonZeroValues);

        log.info("Successfully created embedding with remote model: {} dimensions", embedding.size());
    }

    /**
     * Test batch document ingestion with remote model
     * Verifies ML Commons can handle multiple inference requests efficiently
     */
    @SneakyThrows
    public void testBatchDocumentIngestionWithRemoteModel() {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create pipeline with batch size for efficient ML Commons processing
        String pipelineConfig = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"description\": \"Batch processing pipeline with remote model\",\n"
                + "  \"processors\": [\n"
                + "    {\n"
                + "      \"text_embedding\": {\n"
                + "        \"model_id\": \"%s\",\n"
                + "        \"field_map\": {\n"
                + "          \"%s\": \"%s\"\n"
                + "        },\n"
                + "        \"batch_size\": 3\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            remoteModelId,
            TEXT_FIELD_NAME,
            EMBEDDING_FIELD_NAME
        );

        // Create pipeline using the raw JSON configuration
        createPipelineProcessor(pipelineConfig, PIPELINE_NAME, remoteModelId, null);

        // Create index
        String indexMapping = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"settings\": {\n"
                + "    \"index.knn\": true,\n"
                + "    \"default_pipeline\": \"%s\",\n"
                + "    \"refresh_interval\": \"1s\"\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"knn_vector\",\n"
                + "        \"dimension\": %d,\n"
                + "        \"method\": {\n"
                + "          \"name\": \"hnsw\",\n"
                + "          \"engine\": \"lucene\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"doc_id\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            PIPELINE_NAME,
            EMBEDDING_FIELD_NAME,
            EMBEDDING_DIMENSION,
            TEXT_FIELD_NAME
        );

        createIndexWithConfiguration(INDEX_NAME, indexMapping, PIPELINE_NAME);

        // Use bulk API to test batch processing through ML Commons
        String bulkRequestBody = "";
        for (int i = 1; i <= 5; i++) {
            bulkRequestBody += String.format(Locale.ROOT, "{\"index\": {\"_index\": \"%s\", \"_id\": \"%d\"}}\n", INDEX_NAME, i);
            bulkRequestBody += String.format(
                Locale.ROOT,
                "{\"%s\": \"Document %d: Testing batch inference with ML Commons remote model\", \"doc_id\": \"doc_%d\"}\n",
                TEXT_FIELD_NAME,
                i,
                i
            );
        }

        // Execute bulk request using makeRequest
        Response bulkResponse = makeRequest(
            client(),
            "POST",
            "/_bulk?refresh=true",
            null,
            toHttpEntity(bulkRequestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        assertEquals(200, bulkResponse.getStatusLine().getStatusCode());
        assertEquals("All documents should be indexed", 5, getDocCount(INDEX_NAME));

        // Verify all embeddings were created by remote model
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> doc = getDocById(INDEX_NAME, String.valueOf(i));
            assertNotNull("Document " + i + " should exist", doc);

            Map<String, Object> source = (Map<String, Object>) doc.get("_source");
            List<Number> embedding = (List<Number>) source.get(EMBEDDING_FIELD_NAME);

            assertNotNull("Document " + i + " should have embedding from remote model", embedding);
            assertEquals("Remote model embedding dimensions", EMBEDDING_DIMENSION, embedding.size());

            // Verify doc_id field
            assertEquals("doc_" + i, source.get("doc_id"));
        }

        log.info("Successfully processed {} documents in batch with remote model", 5);
    }

    /**
     * Test reindexing with remote model
     * Verifies that documents can be reindexed using ML Commons remote model for embedding generation
     */
    @SneakyThrows
    public void testReindexingWithRemoteModel() {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Step 1: Create source index without embeddings
        String sourceIndex = "source_index";
        String sourceMapping = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"settings\": {\n"
                + "    \"number_of_shards\": 1,\n"
                + "    \"refresh_interval\": \"1s\"\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"title\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"category\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            TEXT_FIELD_NAME
        );

        createIndexWithConfiguration(sourceIndex, sourceMapping, null);

        // Index documents without embeddings
        String doc1 = String.format(
            Locale.ROOT,
            "{\"%s\": \"Machine learning algorithms analyze data patterns\", \"title\": \"ML Basics\", \"category\": \"technology\"}",
            TEXT_FIELD_NAME
        );
        String doc2 = String.format(
            Locale.ROOT,
            "{\"%s\": \"Neural networks mimic human brain structure\", \"title\": \"Deep Learning\", \"category\": \"technology\"}",
            TEXT_FIELD_NAME
        );
        String doc3 = String.format(
            Locale.ROOT,
            "{\"%s\": \"Search engines help find relevant information\", \"title\": \"Search Technology\", \"category\": \"search\"}",
            TEXT_FIELD_NAME
        );

        ingestDocument(sourceIndex, doc1, "1");
        ingestDocument(sourceIndex, doc2, "2");
        ingestDocument(sourceIndex, doc3, "3");

        assertEquals(3, getDocCount(sourceIndex));

        // Step 2: Create pipeline with remote model for target index
        String reindexPipelineName = "reindex_pipeline";
        String pipelineConfig = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"description\": \"Reindex pipeline with remote model embeddings\",\n"
                + "  \"processors\": [\n"
                + "    {\n"
                + "      \"text_embedding\": {\n"
                + "        \"model_id\": \"%s\",\n"
                + "        \"field_map\": {\n"
                + "          \"%s\": \"%s\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            remoteModelId,
            TEXT_FIELD_NAME,
            EMBEDDING_FIELD_NAME
        );

        createPipelineProcessor(pipelineConfig, reindexPipelineName, remoteModelId, null);

        // Step 3: Create target index with vector field
        String targetIndex = "target_index_with_embeddings";
        String targetMapping = String.format(
            Locale.ROOT,
            "{\n"
                + "  \"settings\": {\n"
                + "    \"index.knn\": true,\n"
                + "    \"number_of_shards\": 1,\n"
                + "    \"default_pipeline\": \"%s\"\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"knn_vector\",\n"
                + "        \"dimension\": %d,\n"
                + "        \"method\": {\n"
                + "          \"name\": \"hnsw\",\n"
                + "          \"engine\": \"lucene\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"%s\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"title\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"category\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            reindexPipelineName,
            EMBEDDING_FIELD_NAME,
            EMBEDDING_DIMENSION,
            TEXT_FIELD_NAME
        );

        createIndexWithConfiguration(targetIndex, targetMapping, reindexPipelineName);

        // Step 4: Perform reindexing - this will use ML Commons remote model to generate embeddings
        reindex(sourceIndex, targetIndex);

        // Step 5: Verify reindexing with embeddings
        assertEquals("All documents should be reindexed", 3, getDocCount(targetIndex));

        // Verify each document has embeddings generated by remote model
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> doc = getDocById(targetIndex, String.valueOf(i));
            assertNotNull("Reindexed document " + i + " should exist", doc);

            Map<String, Object> source = (Map<String, Object>) doc.get("_source");

            // Verify original fields are preserved
            assertNotNull("Text field should be preserved", source.get(TEXT_FIELD_NAME));
            assertNotNull("Title should be preserved", source.get("title"));
            assertNotNull("Category should be preserved", source.get("category"));

            // Verify embedding was added by remote model
            List<Number> embedding = (List<Number>) source.get(EMBEDDING_FIELD_NAME);
            assertNotNull("Remote model should add embedding during reindex", embedding);
            assertEquals("Remote model embedding dimensions", EMBEDDING_DIMENSION, embedding.size());
        }

        // Step 6: Verify neural search works on reindexed documents
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(EMBEDDING_FIELD_NAME)
            .queryText("artificial intelligence and machine learning")
            .modelId(remoteModelId)
            .k(2)
            .build();

        Map<String, Object> searchResponse = search(targetIndex, neuralQuery, 2);
        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> searchHits = (List<Map<String, Object>>) hits.get("hits");

        assertNotNull("Should find results with neural search", searchHits);
        assertTrue("Should find at least one document", searchHits.size() > 0);

        log.info("Successfully reindexed {} documents with remote model embeddings", 3);

        // Cleanup
        deleteIndex(sourceIndex);
        deleteIndex(targetIndex);
        deleteIngestPipeline(reindexPipelineName);
    }

    /**
     * Test semantic field with remote symmetric model for both ingestion and search
     * Demonstrates end-to-end semantic search using the semantic field type
     */
    @SneakyThrows
    public void testSemanticFieldWithRemoteModel() {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        String semanticIndex = "semantic_field_test_index";

        try {

            // Step 1: Create index with semantic field mapping
            String indexMapping = String.format(Locale.ROOT, """
                {
                  "settings": {
                    "index.knn": true
                  },
                  "mappings": {
                    "properties": {
                      "title": {
                        "type": "text"
                      },
                      "content": {
                        "type": "semantic",
                        "model_id": "%s",
                        "dense_embedding_config": {
                          "method": {
                            "engine": "lucene"
                          }
                        }
                      },
                      "category": {
                        "type": "keyword"
                      }
                    }
                  }
                }
                """, remoteModelId);

            createIndexWithConfiguration(semanticIndex, indexMapping, null);

            // Step 2: Ingest documents with semantic field
            String doc1 =
                """
                    {
                      "title": "Introduction to Machine Learning",
                      "content": "Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed.",
                      "category": "AI"
                    }
                    """;

            String doc2 =
                """
                    {
                      "title": "Deep Learning Fundamentals",
                      "content": "Deep learning is part of machine learning methods based on artificial neural networks with representation learning.",
                      "category": "AI"
                    }
                    """;

            String doc3 =
                """
                    {
                      "title": "Search Engine Optimization",
                      "content": "SEO is the process of improving the quality and quantity of website traffic to a website or a web page from search engines.",
                      "category": "Web"
                    }
                    """;

            String doc4 =
                """
                    {
                      "title": "Natural Language Processing",
                      "content": "NLP is a branch of artificial intelligence that helps computers understand, interpret and manipulate human language.",
                      "category": "AI"
                    }
                    """;

            // Ingest documents
            try {
                ingestDocument(semanticIndex, doc1, "1");
                ingestDocument(semanticIndex, doc2, "2");
                ingestDocument(semanticIndex, doc3, "3");
                ingestDocument(semanticIndex, doc4, "4");

                assertEquals(4, getDocCount(semanticIndex));
            } catch (Exception e) {
                log.error("Failed to ingest documents: ", e);
                throw e;
            }

            // Step 3: Verify semantic field created embeddings
            Map<String, Object> doc = getDocById(semanticIndex, "1");
            Map<String, Object> source = (Map<String, Object>) doc.get("_source");

            // Semantic fields store text directly and embeddings in a separate structure
            String contentText = (String) source.get("content");
            assertNotNull("Semantic field text should exist", contentText);
            assertTrue("Original text should contain expected content", contentText.contains("Machine learning"));

            // The embedding is stored in content_semantic_info.embedding
            Map<String, Object> semanticInfo = (Map<String, Object>) source.get("content_semantic_info");
            assertNotNull("Semantic info should exist", semanticInfo);

            List<Number> embedding = (List<Number>) semanticInfo.get("embedding");
            assertNotNull("KNN embedding should exist", embedding);
            assertEquals("Embedding dimension should match", EMBEDDING_DIMENSION, embedding.size());

            // Verify model info exists
            Map<String, Object> modelInfo = (Map<String, Object>) semanticInfo.get("model");
            assertNotNull("Model info should exist", modelInfo);

            // Step 4: Perform neural search on semantic field
            // For semantic fields, we need to query the embedding field directly
            NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
                .fieldName("content")
                .queryText("artificial intelligence and machine learning concepts")
                .k(3)
                .build();

            Map<String, Object> searchResponse = search(semanticIndex, neuralQuery, 3);
            Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
            List<Map<String, Object>> searchHits = (List<Map<String, Object>>) hits.get("hits");

            assertNotNull("Should find results", searchHits);
            assertEquals("Should find top 3 AI-related documents", 3, searchHits.size());

            // Verify scoring - first result should have highest score
            double firstScore = (double) searchHits.get(0).get("_score");
            double secondScore = (double) searchHits.get(1).get("_score");
            assertTrue("First result should have higher score than second", firstScore >= secondScore);

            log.info("Successfully tested semantic field with remote symmetric model");

        } finally {
            // Cleanup
            try {
                deleteIndex(semanticIndex);
            } catch (Exception e) {
                log.debug("Failed to delete index during cleanup: {}", e.getMessage());
            }
        }
    }

    /**
     * Override deployRemoteModel to split registration and deployment
     * This matches the pattern used in AsymmetricRemoteModelIT which works
     */
    @Override
    protected String deployRemoteModel(String connectorId, String modelName) throws Exception {
        String requestBody = String.format(Locale.ROOT, """
            {
                "name": "%s",
                "function_name": "remote",
                "description": "Remote symmetric text embedding model",
                "connector_id": "%s",
                "model_config": {
                    "model_type": "TEXT_EMBEDDING",
                    "embedding_dimension": 128,
                    "framework_type": "sentence_transformers",
                    "additional_config": {
                        "space_type": "l2"
                    }
                }
            }
            """, modelName, connectorId);

        // Register the model first
        Request request = new Request("POST", "/_plugins/_ml/models/_register");
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            false
        );

        String modelId = (String) responseMap.get("model_id");

        // Then deploy it separately
        Request deployRequest = new Request("POST", "/_plugins/_ml/models/" + modelId + "/_deploy");
        client().performRequest(deployRequest);

        // Wait for model to be deployed
        waitForModelToBeReady(modelId);

        return modelId;
    }

    /**
     * Create a TorchServe connector for text embedding
     * Overrides the base method to use proper preprocessing for text embedding
     */
    @Override
    protected String createRemoteModelConnector(String endpoint) throws Exception {
        String connectorName = "symmetric-connector-" + System.currentTimeMillis();
        String connectorTemplate = Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("symmetric/RemoteTorchServeConnector.json")).toURI())
        );
        String requestBody = String.format(Locale.ROOT, connectorTemplate, connectorName, endpoint);

        Request request = new Request("POST", "/_plugins/_ml/connectors/_create");
        request.setJsonEntity(requestBody);

        Response response = client().performRequest(request);
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            false
        );

        return (String) responseMap.get("connector_id");
    }

}
