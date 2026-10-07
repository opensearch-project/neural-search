/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.it;

import com.google.common.collect.ImmutableList;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * Integration tests for the {@code model_selection} parameter on the semantic field. The model_id is resolved from the
 * {@code plugins.neural_search.model_selection.model_id.*} cluster settings.
 */
public class SemanticFieldModelSelectionIT extends BaseNeuralSearchIT {
    private static final String SPARSE_ENGLISH_SETTING_KEY = "plugins.neural_search.model_selection.model_id.sparse.english";
    private static final String DENSE_ENGLISH_SETTING_KEY = "plugins.neural_search.model_selection.model_id.dense.english";

    /**
     * When model_selection is used but no model_id is configured for the profile, index creation fails with a clear error.
     */
    public void testCreateIndex_withModelSelection_whenNoModelConfigured_thenFail() throws Exception {
        final String indexBody = "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"content\": {\n"
            + "        \"type\": \"semantic\",\n"
            + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        final ResponseException exception = assertThrows(
            ResponseException.class,
            () -> createIndex("semantic_model_selection_no_config", indexBody)
        );
        final Response response = exception.getResponse();
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
        final String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue("Response should explain that no model_id is configured", responseBody.contains("No model_id is configured"));
    }

    /**
     * When model_selection is used and the matching model_id is configured, the index is created and the resolved
     * model_id is written into the mapping alongside the model_selection.
     */
    @SuppressWarnings("unchecked")
    public void testCreateIndex_withModelSelection_whenModelConfigured_thenResolveModelId() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexName = "semantic_model_selection_ok";
            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            createIndex(indexName, indexBody);

            final Map<String, Object> contentField = getSemanticContentFieldMapping(indexName);
            assertEquals(modelId, contentField.get("model_id"));
            assertTrue(contentField.get("model_selection") instanceof Map);
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * When both model_id and model_selection are provided but they resolve to different models, index creation fails.
     */
    public void testCreateIndex_withModelSelectionAndMismatchedModelId_thenFail() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_id\": \"some_other_model_id\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            final ResponseException exception = assertThrows(
                ResponseException.class,
                () -> createIndex("semantic_model_selection_mismatch", indexBody)
            );
            final Response response = exception.getResponse();
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            final String responseBody = EntityUtils.toString(response.getEntity());
            assertTrue("Response should explain the model_id mismatch", responseBody.contains("does not match the model_id"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * Dense path: when a dense model is configured for the dense/english profile, model_selection resolves to it.
     */
    @SuppressWarnings("unchecked")
    public void testCreateIndex_withModelSelection_denseEnglish_whenModelConfigured_thenResolveModelId() throws Exception {
        final String modelId = prepareModel();
        try {
            updateClusterSettings(DENSE_ENGLISH_SETTING_KEY, modelId);

            final String indexName = "semantic_model_selection_dense_ok";
            // Dense semantic fields build a knn_vector companion field, which requires index.knn = true.
            final String indexBody = "{\n"
                + "  \"settings\": { \"index.knn\": true },\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"DENSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            createIndex(indexName, indexBody);

            final Map<String, Object> contentField = getSemanticContentFieldMapping(indexName);
            assertEquals(modelId, contentField.get("model_id"));
            assertTrue(contentField.get("model_selection") instanceof Map);
        } finally {
            updateClusterSettings(DENSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * When both model_id and model_selection are provided and they resolve to the same model, the index is created.
     */
    @SuppressWarnings("unchecked")
    public void testCreateIndex_withModelSelectionAndMatchingModelId_thenSucceed() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexName = "semantic_model_selection_match_ok";
            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_id\": \""
                + modelId
                + "\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            createIndex(indexName, indexBody);

            final Map<String, Object> contentField = getSemanticContentFieldMapping(indexName);
            assertEquals(modelId, contentField.get("model_id"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * When the configured model's type does not match the requested model_type, index creation fails.
     */
    public void testCreateIndex_withModelSelection_whenModelTypeMismatch_thenFail() throws Exception {
        // Configure a SPARSE model under the DENSE/english profile, then request DENSE.
        final String sparseModelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(DENSE_ENGLISH_SETTING_KEY, sparseModelId);

            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"DENSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            final ResponseException exception = assertThrows(
                ResponseException.class,
                () -> createIndex("semantic_model_selection_type_mismatch", indexBody)
            );
            final Response response = exception.getResponse();
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            final String responseBody = EntityUtils.toString(response.getEntity());
            assertTrue("Response should explain the model_type mismatch", responseBody.contains("does not match the requested model_type"));
        } finally {
            updateClusterSettings(DENSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * When model_id is not a string, index creation fails with a clear error.
     */
    public void testCreateIndex_withModelSelectionAndNonStringModelId_thenFail() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_id\": { \"unexpected\": \"object\" },\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            final ResponseException exception = assertThrows(
                ResponseException.class,
                () -> createIndex("semantic_model_selection_non_string_model_id", indexBody)
            );
            final Response response = exception.getResponse();
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            final String responseBody = EntityUtils.toString(response.getEntity());
            assertTrue("Response should explain model_id must be a string", responseBody.contains("must be a string"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * A model_selection field with an invalid semantic_info_field_name (contains '.') is rejected, consistent with the
     * explicit model_id path.
     */
    public void testCreateIndex_withModelSelectionAndInvalidSemanticInfoFieldName_thenFail() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexBody = "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"content\": {\n"
                + "        \"type\": \"semantic\",\n"
                + "        \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" },\n"
                + "        \"semantic_info_field_name\": \"bad.name\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

            final ResponseException exception = assertThrows(
                ResponseException.class,
                () -> createIndex("semantic_model_selection_bad_info_field", indexBody)
            );
            final Response response = exception.getResponse();
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            final String responseBody = EntityUtils.toString(response.getEntity());
            assertTrue("Response should reject '.' in semantic_info_field_name", responseBody.contains("should not contain '.'"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * Adding a semantic field with model_selection via the update-mapping API (PUT _mapping) resolves the model_id.
     */
    public void testUpdateMapping_addModelSelectionField_thenResolveModelId() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexName = "semantic_model_selection_put_mapping";
            createIndex(indexName, "{\n  \"mappings\": { \"properties\": { \"title\": { \"type\": \"text\" } } }\n}");

            updateIndexMapping(
                indexName,
                "{\n  \"properties\": {\n    \"content\": {\n      \"type\": \"semantic\",\n"
                    + "      \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                    + "    }\n  }\n}"
            );

            final Map<String, Object> contentField = getSemanticContentFieldMapping(indexName);
            assertEquals(modelId, contentField.get("model_id"));
            assertTrue(contentField.get("model_selection") instanceof Map);
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * A composable index template with a model_selection semantic field resolves the model_id, and an index created
     * from that template carries the resolved model_id.
     */
    public void testComposableTemplate_withModelSelection_thenResolveModelId() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        final String templateName = "semantic_model_selection_template";
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String templateBody = "{\n"
                + "  \"index_patterns\": [\"semantic-ms-*\"],\n"
                + "  \"template\": { \"mappings\": { \"properties\": {\n"
                + "    \"content\": { \"type\": \"semantic\", \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" } }\n"
                + "  } } }\n"
                + "}";
            makeRequest(
                client(),
                "PUT",
                "_index_template/" + templateName,
                null,
                toHttpEntity(templateBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
            );

            final String indexName = "semantic-ms-1";
            createIndex(indexName, "{}");

            final Map<String, Object> contentField = getSemanticContentFieldMapping(indexName);
            assertEquals(modelId, contentField.get("model_id"));
            assertTrue(contentField.get("model_selection") instanceof Map);
        } finally {
            try {
                makeRequest(
                    client(),
                    "DELETE",
                    "_index_template/" + templateName,
                    null,
                    null,
                    ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
                );
            } catch (Exception ignored) {}
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * After the operator rotates the profile's model_id, reapplying a previously read-back mapping (which carries the
     * old model_id alongside model_selection) fails with a clear mismatch error.
     */
    public void testReapplyMappingAfterProfileRotation_thenFail() throws Exception {
        final String modelId = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId);

            final String indexName = "semantic_model_selection_rotation";
            createIndex(
                indexName,
                "{\n  \"mappings\": { \"properties\": {\n"
                    + "    \"content\": { \"type\": \"semantic\", \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" } }\n"
                    + "  } }\n}"
            );
            // The field resolved to the current profile model.
            assertEquals(modelId, getSemanticContentFieldMapping(indexName).get("model_id"));

            // Operator rotates the profile to a different model_id.
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, "rotated_model_id");

            // Reapplying the read-back mapping (old model_id + model_selection) now conflicts with the rotated profile.
            final String reapplyBody = "{\n  \"properties\": {\n    \"content\": {\n      \"type\": \"semantic\",\n"
                + "      \"model_id\": \""
                + modelId
                + "\",\n"
                + "      \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                + "    }\n  }\n}";

            final ResponseException exception = assertThrows(ResponseException.class, () -> updateIndexMapping(indexName, reapplyBody));
            final Response response = exception.getResponse();
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            final String responseBody = EntityUtils.toString(response.getEntity());
            assertTrue("Response should explain the model_id mismatch", responseBody.contains("does not match the model_id"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    /**
     * Canary pinning today's behavior: after the operator rotates the profile to a new model, re-applying a
     * model_selection-only mapping (no model_id) silently rebinds the field to the new model and returns 200.
     * opensearch-project/neural-search#1963 will make the model binding immutable; when that lands this should instead
     * be rejected, turning this test red as a signal of the behavior change.
     */
    @SuppressWarnings("unchecked")
    public void testReapplyModelSelectionOnly_afterRotation_silentlyRebinds() throws Exception {
        final String modelId1 = prepareSparseEncodingModel();
        final String modelId2 = prepareSparseEncodingModel();
        try {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId1);

            final String indexName = "semantic_model_selection_silent_rebind";
            createIndex(
                indexName,
                "{\n  \"mappings\": { \"properties\": {\n"
                    + "    \"content\": { \"type\": \"semantic\", \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" } }\n"
                    + "  } }\n}"
            );
            assertEquals(modelId1, getSemanticContentFieldMapping(indexName).get("model_id"));

            // Operator rotates the profile to a second (same-type) model.
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, modelId2);

            // Re-apply with model_selection ONLY (no model_id). Today this succeeds (200) and rebinds to modelId2.
            updateIndexMapping(
                indexName,
                "{\n  \"properties\": {\n    \"content\": {\n      \"type\": \"semantic\",\n"
                    + "      \"model_selection\": { \"language_option\": \"ENGLISH\", \"model_type\": \"SPARSE\" }\n"
                    + "    }\n  }\n}"
            );

            assertEquals(modelId2, getSemanticContentFieldMapping(indexName).get("model_id"));
        } finally {
            updateClusterSettings(SPARSE_ENGLISH_SETTING_KEY, null);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSemanticContentFieldMapping(final String indexName) {
        final Map<String, Object> indexMapping = getIndexMapping(indexName);
        final Map<String, Object> index = (Map<String, Object>) indexMapping.get(indexName);
        final Map<String, Object> mappings = (Map<String, Object>) index.get("mappings");
        final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        return (Map<String, Object>) properties.get("content");
    }
}
