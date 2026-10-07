/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;

import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * End-to-end integration test for the semantic field mapping transformation with a remote dense
 * (text embedding) model served by the TorchServe Docker mock. It verifies two things:
 * <ol>
 *   <li>The semantic field mapping is transformed correctly using the embedding dimension and
 *       space type resolved from the deployed remote model.</li>
 *   <li>The remote model actually generates embeddings during ingestion (real inference through
 *       the ML Commons connector), not just a metadata-only mapping change.</li>
 * </ol>
 * This test is only exercised by the {@code remoteModelIntegTest} Gradle task (class name matches
 * the {@code *RemoteModelIT*} filter) and is skipped when TorchServe is not available.
 */
@Log4j2
public class SemanticMappingTransformerRemoteModelIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "semantic_field_remote_dense_model_index";
    private static final int EMBEDDING_DIMENSION = 128; // tiny BERT model served by TorchServe emits 128-dim embeddings

    // Nested semantic field structure produced by mappingtransformer/SemanticIndexMappings.json
    private static final String LEVEL_1_FIELD = "products";
    private static final String SEMANTIC_INFO_FIELD = "product_description_semantic_info";
    private static final String EMBEDDING_FIELD = "embedding";

    private final String createIndexRequestBody = Files.readString(
        Path.of(Objects.requireNonNull(classLoader.getResource("mappingtransformer/SemanticIndexMappings.json")).toURI())
    );
    private final String expectedIndexMappingTemplate = Files.readString(
        Path.of(Objects.requireNonNull(classLoader.getResource("mappingtransformer/expectedIndexMappingWithRemoteDenseModel.json")).toURI())
    );
    private final String ingestDoc = Files.readString(
        Path.of(Objects.requireNonNull(classLoader.getResource("mappingtransformer/ingest_doc_remote_dense_model.json")).toURI())
    );

    private String connectorId;
    private String remoteModelId;
    private boolean isTorchServeAvailable = false;

    public SemanticMappingTransformerRemoteModelIT() throws IOException, URISyntaxException {}

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
            List.of("^http://localhost:.*", "^http://127\\.0\\.0\\.1:.*", "^http://torchserve:.*")
        );

        String torchServeEndpoint = System.getenv("TORCHSERVE_ENDPOINT");
        if (torchServeEndpoint == null) {
            torchServeEndpoint = System.getProperty("tests.torchserve.endpoint");
        }

        if (torchServeEndpoint == null || torchServeEndpoint.isEmpty()) {
            log.info("TorchServe endpoint not configured, tests will be skipped");
            return;
        }

        isTorchServeAvailable = RemoteModelTestUtils.isRemoteEndpointAvailable(torchServeEndpoint);
        if (!isTorchServeAvailable) {
            log.info("TorchServe not available at {}, tests will be skipped", torchServeEndpoint);
            return;
        }

        log.info("TorchServe endpoint available at: {}", torchServeEndpoint);
        try {
            connectorId = createRemoteModelConnector(torchServeEndpoint);
            log.info("Created connector with ID: {}", connectorId);

            remoteModelId = deployRemoteModel(connectorId, "semantic-mapping-transformer-dense-remote");
            log.info("Deployed remote text embedding model with ID: {}", remoteModelId);
        } catch (Exception e) {
            log.error("Failed to set up remote model: ", e);
            isTorchServeAvailable = false;
        }
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();

        try {
            deleteIndex(INDEX_NAME);
        } catch (Exception e) {
            log.debug("Index cleanup failed: {}", e.getMessage());
        }

        if (remoteModelId != null || connectorId != null) {
            cleanupRemoteModelResources(connectorId, remoteModelId);
        }
    }

    /**
     * Verifies that the semantic field mapping is transformed using the dimension/space type from the
     * deployed remote dense model, and that the remote model produces real embeddings during ingestion.
     */
    @SneakyThrows
    public void testTransformMappingWithRemoteDenseModel() {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // 1. Create the semantic index. The mapping transformer resolves the embedding dimension and
        // space type from the deployed remote model to build the knn_vector sub-field.
        createSemanticIndexWithConfiguration(INDEX_NAME, createIndexRequestBody, remoteModelId);

        // 2. Assert the transformed index mapping matches the expected 128-dim dense mapping.
        final Map<String, Object> indexMapping = getIndexMapping(INDEX_NAME);
        final String expectedIndexMappingStr = String.format(Locale.ROOT, expectedIndexMappingTemplate, remoteModelId);
        final Map<String, Object> expectedIndexMappingMap = createParser(XContentType.JSON.xContent(), expectedIndexMappingStr).map();
        org.assertj.core.api.Assertions.assertThat(indexMapping).isEqualTo(expectedIndexMappingMap);

        // 3. Ingest a document and verify the remote model actually generated embeddings.
        ingestDocument(INDEX_NAME, ingestDoc, "1");
        assertEquals(1, getDocCount(INDEX_NAME));

        @SuppressWarnings("unchecked")
        final Map<String, Object> source = (Map<String, Object>) getDocById(INDEX_NAME, "1").get("_source");
        assertEmbeddingsGenerated(source);
    }

    /**
     * Verifies each nested product has a 128-dim, non-zero embedding produced by the remote model.
     */
    @SuppressWarnings("unchecked")
    private void assertEmbeddingsGenerated(final Map<String, Object> source) {
        final List<Map<String, Object>> products = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        assertNotNull("Nested products should exist", products);
        assertEquals("Both nested products should be present", 2, products.size());

        for (final Map<String, Object> product : products) {
            final Map<String, Object> semanticInfo = (Map<String, Object>) product.get(SEMANTIC_INFO_FIELD);
            assertNotNull("Semantic info should exist for each product", semanticInfo);

            final List<Number> embedding = (List<Number>) semanticInfo.get(EMBEDDING_FIELD);
            assertNotNull("Remote model should generate an embedding", embedding);
            assertEquals("Embedding dimension should match the remote model", EMBEDDING_DIMENSION, embedding.size());

            final boolean hasNonZeroValues = embedding.stream().anyMatch(value -> value.doubleValue() != 0.0);
            assertTrue("Embedding should contain non-zero values", hasNonZeroValues);
        }
    }

    /**
     * Registers and deploys the remote model with a full text-embedding {@code model_config}. The
     * mapping transformer reads {@code embedding_dimension} and {@code additional_config.space_type}
     * from this config, so they must match the TorchServe dense handler (128-dim, l2).
     */
    @Override
    protected String deployRemoteModel(final String connectorId, final String modelName) throws Exception {
        final String requestBody = String.format(Locale.ROOT, """
            {
                "name": "%s",
                "function_name": "remote",
                "description": "Remote dense text embedding model for semantic mapping transformer IT",
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

        final Request registerRequest = new Request("POST", "/_plugins/_ml/models/_register");
        registerRequest.setJsonEntity(requestBody);
        final Response registerResponse = client().performRequest(registerRequest);
        final Map<String, Object> registerResponseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            registerResponse.getEntity().getContent(),
            false
        );
        final String modelId = (String) registerResponseMap.get("model_id");

        final Request deployRequest = new Request("POST", "/_plugins/_ml/models/" + modelId + "/_deploy");
        client().performRequest(deployRequest);

        waitForModelToBeReady(modelId);
        return modelId;
    }

    /**
     * Creates a TorchServe connector for symmetric (dense) text embedding, reusing the shared
     * connector template used by {@link org.opensearch.neuralsearch.ml.SymmetricRemoteModelIT}.
     */
    @Override
    protected String createRemoteModelConnector(final String endpoint) throws Exception {
        final String connectorName = "semantic-mapping-transformer-dense-connector-" + System.currentTimeMillis();
        final String connectorTemplate = Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("symmetric/RemoteTorchServeConnector.json")).toURI())
        );
        final String requestBody = String.format(Locale.ROOT, connectorTemplate, connectorName, endpoint);

        final Response response = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/connectors/_create",
            null,
            toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            false
        );
        return (String) responseMap.get("connector_id");
    }
}
