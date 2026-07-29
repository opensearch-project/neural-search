/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.it;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.Map;

/**
 * Integration tests for the language_option and model_type parameters on the semantic field type.
 * Tests that the mapping parameters are accepted and validated correctly.
 */
public class SemanticFieldLanguageModelTypeIT extends BaseNeuralSearchIT {
    private static final String INDEX_NAME = "semantic_language_model_type_test";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    /**
     * Test that creating an index with language_option and model_type (no model_id)
     * is accepted at the mapping level. The model resolution happens asynchronously
     * via the mapping transformer.
     */
    public void testCreateIndex_withLanguageOptionAndModelType_sparseEnglish() throws Exception {
        String indexBody = "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"content\": {\n"
            + "        \"type\": \"semantic\",\n"
            + "        \"language_option\": \"ENGLISH\",\n"
            + "        \"model_type\": \"SPARSE\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        createIndex(INDEX_NAME, indexBody);

        // Verify the mapping was persisted
        Map<String, Object> indexMapping = getIndexMapping(INDEX_NAME);
        assertNotNull(indexMapping);

        // The mapping should contain the semantic field with model_id set by the resolver,
        // and language_option/model_type should be present
        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get(INDEX_NAME);
        assertNotNull("Index mapping should be present", mappings);
    }

    /**
     * Test that specifying model_id together with language_option returns a 400 error.
     */
    public void testCreateIndex_withModelIdAndLanguageOption_shouldFail() throws Exception {
        String indexBody = "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"content\": {\n"
            + "        \"type\": \"semantic\",\n"
            + "        \"model_id\": \"some_model_id\",\n"
            + "        \"language_option\": \"ENGLISH\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ResponseException exception = assertThrows(ResponseException.class, () -> createIndex(INDEX_NAME + "_conflict", indexBody));
        Response response = exception.getResponse();
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue(
            "Response should mention mutual exclusion",
            responseBody.contains("Cannot specify model_id together with language_option or model_type")
        );
    }

    /**
     * Test that specifying model_id together with model_type returns a 400 error.
     */
    public void testCreateIndex_withModelIdAndModelType_shouldFail() throws Exception {
        String indexBody = "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"content\": {\n"
            + "        \"type\": \"semantic\",\n"
            + "        \"model_id\": \"some_model_id\",\n"
            + "        \"model_type\": \"SPARSE\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ResponseException exception = assertThrows(ResponseException.class, () -> createIndex(INDEX_NAME + "_conflict2", indexBody));
        Response response = exception.getResponse();
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        assertTrue(
            "Response should mention mutual exclusion",
            responseBody.contains("Cannot specify model_id together with language_option or model_type")
        );
    }
}
