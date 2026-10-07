/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

/**
 * Integration tests for sparse index feature
 */
public class SparseIndexingIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String NON_SPARSE_TEST_INDEX_NAME = TEST_INDEX_NAME + "_non_sparse";
    private static final String INVALID_PARAM_TEST_INDEX_NAME = TEST_INDEX_NAME + "_invalid";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String PIPELINE_NAME = "seismic_test_pipeline";
    private static final List<String> TEST_TOKENS = List.of("1000", "2000", "3000", "4000", "5000");

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return allEngines();
    }

    public SparseIndexingIT(SparseEngine engine) {
        super(engine);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Test creating an index with sparse index setting enabled
     */
    public void testCreateSparseIndex() throws IOException {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, 8);

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        assertEquals("true", indexSettingsMap.get("sparse"));

        // The default engine is left out of the mapping the cluster returns, which is what keeps a
        // pre-3.9 index's mapping source stable across an upgrade; see SparseMethodContext#toXContent.
        Object engineInMapping = getSparseMethodConfig(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME).get(SparseConstants.ENGINE_FIELD);
        assertEquals(engine == SparseEngine.DEFAULT ? null : engine.getName(), engineInMapping);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSparseMethodConfig(String indexName, String fieldName) {
        Map<String, Object> indexMapping = getIndexMapping(indexName);
        Map<String, Object> mappings = (Map<String, Object>) ((Map<String, Object>) indexMapping.get(indexName)).get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        Map<String, Object> fieldConfig = (Map<String, Object>) properties.get(fieldName);
        return (Map<String, Object>) fieldConfig.get("method");
    }

    /**
     * Test indexing documents with sparse vector field
     */
    public void testIndexDocumentsWithSparseVectorField() throws IOException {
        // Create index with sparse index setting enabled
        testCreateSparseIndex();

        // Create a document with sparse vector field
        Map<String, Float> sparseTokens = createRandomTokenWeightMap(TEST_TOKENS);

        // Index the document
        addSparseEncodingDoc(TEST_INDEX_NAME, "1", List.of(TEST_SPARSE_FIELD_NAME), List.of(sparseTokens));

        // Verify document was indexed
        assertEquals(1, getDocCount(TEST_INDEX_NAME));

        // Get the document and verify its content
        Map<String, Object> document = getDocById(TEST_INDEX_NAME, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        Map<String, Object> sparseField = (Map<String, Object>) source.get(TEST_SPARSE_FIELD_NAME);
        assertNotNull(sparseField);

        // Verify the sparse tokens are present
        for (String token : TEST_TOKENS) {
            if (sparseTokens.containsKey(token)) {
                assertTrue(sparseField.containsKey(token));
                assertEquals(sparseTokens.get(token).doubleValue(), ((Number) sparseField.get(token)).doubleValue(), 0.001);
            }
        }
    }

    /**
     * Test creating an index with sparse index setting disabled (default)
     */
    public void testCreateNonSparseIndex() throws IOException {
        // Create index without sparse index setting (default is false)
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
        String indexMappings = prepareIndexMapping(100, 0.4f, 0.1f, 8, TEST_SPARSE_FIELD_NAME);

        Request request = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(NON_SPARSE_TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(NON_SPARSE_TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        // Sparse setting should not be present (default is false)
        assertFalse(indexSettingsMap.containsKey("sparse"));
    }

    /**
     * Test that sparse index setting cannot be updated after index creation (it's a final setting)
     */
    public void testCannotUpdateSparseIndexSetting() throws IOException {
        // Create index without sparse index setting (default is false)
        testCreateNonSparseIndex();

        // Try to update the sparse index setting (should fail because it's final)
        Request updateSettingsRequest = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" + "  \"index\": {\n" + "    \"sparse\": true\n" + "  }\n" + "}");

        // This should throw an exception because sparse is a final setting
        expectThrows(IOException.class, () -> { client().performRequest(updateSettingsRequest); });
    }

    /**
     * Test error handling when creating a sparse tokens field with invalid parameters
     */
    public void testSparseVectorFieldWithInvalidParameters1() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, -1, 0.4f, 0.1f, 8); }
        );
    }

    public void testSparseVectorFieldWithInvalidParameters2() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, -0.4f, 0.1f, 8); }
        );
    }

    public void testSparseVectorFieldWithInvalidParameters3() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, -0.1f, 8); }
        );
    }

    public void testSparseVectorFieldWithInvalidParameters4() throws IOException {
        expectThrows(
            IOException.class,
            () -> { createSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, -8); }
        );
    }

    public void testSparseVectorFieldWithAdditionParameters() throws IOException {
        // Create index with sparse index setting enabled
        String indexSettings = prepareIndexSettings();
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseVectorFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME)
            .field(SparseConstants.ENGINE_FIELD, engine.getName())
            .startObject("parameters")
            .field("n_postings", 100) // Integer: length of posting list
            .field("summary_prune_ratio", 0.1f) // Float: alpha-prune ration for summary
            .field("cluster_ratio", 0.1f) // Float: cluster ratio
            .field("approximate_threshold", 8)
            .field("additional_parameter", 8)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String indexName = TEST_INDEX_NAME + "_method_params";
        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            mappingBuilder.toString()
        );
        request.setJsonEntity(body);
        expectThrows(IOException.class, () -> client().performRequest(request));
    }

    /**
     * Test creating an index with multiple seismic fields
     */
    public void testCreateIndexWithMultipleSeismicFields() throws IOException {
        String field1 = "sparse_field_1";
        String field2 = "sparse_field_2";
        String field3 = "sparse_field_3";

        createIndexWithMultipleSeismicFields(TEST_INDEX_NAME, List.of(field1, field2, field3));

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Verify index mapping contains all sparse fields
        Map<String, Object> indexMapping = getIndexMapping(TEST_INDEX_NAME);
        Map<String, Object> mappings = (Map<String, Object>) indexMapping.get(TEST_INDEX_NAME);
        Map<String, Object> mappingsProperties = (Map<String, Object>) mappings.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappingsProperties.get("properties");

        // Check each sparse field exists in mapping
        assertTrue(properties.containsKey(field1));
        assertTrue(properties.containsKey(field2));
        assertTrue(properties.containsKey(field3));

        // Verify field types are sparse_vector
        Map<String, Object> field1Config = (Map<String, Object>) properties.get(field1);
        Map<String, Object> field2Config = (Map<String, Object>) properties.get(field2);
        Map<String, Object> field3Config = (Map<String, Object>) properties.get(field3);

        assertEquals(SparseVectorFieldMapper.CONTENT_TYPE, field1Config.get("type"));
        assertEquals(SparseVectorFieldMapper.CONTENT_TYPE, field2Config.get("type"));
        assertEquals(SparseVectorFieldMapper.CONTENT_TYPE, field3Config.get("type"));
    }

    /**
     * Test indexing documents with multiple seismic fields
     */
    public void testIndexDocumentsWithMultipleSeismicFields() {
        String field1 = "sparse_field_1";
        String field2 = "sparse_field_2";
        String field3 = "sparse_field_3";

        createIndexWithMultipleSeismicFields(TEST_INDEX_NAME, List.of(field1, field2, field3));

        // Create documents with different sparse tokens for each field using integer tokens
        Map<String, Float> tokens1 = Map.of("1000", 0.1f, "2000", 0.2f);
        Map<String, Float> tokens2 = Map.of("3000", 0.3f, "4000", 0.4f);
        Map<String, Float> tokens3 = Map.of("5000", 0.5f, "6000", 0.6f);

        // Index document with multiple sparse fields
        addSparseEncodingDoc(TEST_INDEX_NAME, "1", List.of(field1, field2, field3), List.of(tokens1, tokens2, tokens3));

        // Verify document was indexed
        assertEquals(1, getDocCount(TEST_INDEX_NAME));

        // Get the document and verify its content
        Map<String, Object> document = getDocById(TEST_INDEX_NAME, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        // Verify all sparse fields are present with correct tokens
        Map<String, Object> sparseField1 = (Map<String, Object>) source.get(field1);
        Map<String, Object> sparseField2 = (Map<String, Object>) source.get(field2);
        Map<String, Object> sparseField3 = (Map<String, Object>) source.get(field3);

        assertNotNull(sparseField1);
        assertNotNull(sparseField2);
        assertNotNull(sparseField3);

        // Verify tokens in each field
        assertEquals(0.1f, ((Number) sparseField1.get("1000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.2f, ((Number) sparseField1.get("2000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.3f, ((Number) sparseField2.get("3000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.4f, ((Number) sparseField2.get("4000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.5f, ((Number) sparseField3.get("5000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.6f, ((Number) sparseField3.get("6000")).floatValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testSeismicIndexWithDocDeletion() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            NON_SPARSE_TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );
        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertEquals(8, getHitCount(searchResults));

        deleteDocById(TEST_INDEX_NAME, "1");

        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertEquals(7, getHitCount(searchResults));
        Set<String> actualIds = new HashSet<>(getDocIDs(searchResults));
        assertEquals(Set.of("2", "3", "4", "5", "6", "7", "8"), actualIds);
    }

    /**
     * Deleting the top scoring documents must not cost the query any of its k results: both engines
     * have to reach past the deleted docs rather than let them consume result slots.
     */
    public void testSeismicIndexWithDeletedTopHitsDoesNotShrinkK() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        // Docs 8 and 7 score highest, so with k = 3 they occupy the result slots until deleted.
        deleteDocById(TEST_INDEX_NAME, "8");
        deleteDocById(TEST_INDEX_NAME, "7");

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            3,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        // The count is the point: if deleted docs consumed result slots this would come back short of
        // k. Which live docs fill the slots is deliberately not asserted -- nsparse seeds its k-means
        // from std::random_device, so the native approximate ranking varies per index build.
        assertEquals(3, getHitCount(searchResults));
        List<String> actualIds = getDocIDs(searchResults);
        assertFalse("deleted docs must not be returned, got " + actualIds, actualIds.contains("7") || actualIds.contains("8"));
    }

    public void testSeismicIndexWithDocUpdate() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 8, 0.4f, 0.5f, 8);

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            NON_SPARSE_TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );
        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertEquals(8, getHitCount(searchResults));

        updateSparseVector(TEST_INDEX_NAME, "1", TEST_SPARSE_FIELD_NAME, Map.of("3000", 0.1f, "4000", 0.2f));

        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertEquals(8, getHitCount(searchResults));

        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 2, 1.0f, 10, Map.of("3000", 0.1f));
        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        Set<String> actualIds = new HashSet<>(getDocIDs(searchResults));
        assertEquals(Set.of("1"), actualIds);
    }

    /**
     * A closed index's shards have no MapperService, so removing that index on reopen used to throw
     * out of SparseIndexEventListener, abort IndicesService#removeIndex and leak the shard lock,
     * leaving the shard unassigned and every search failing with "all shards failed".
     */
    public void testSeismicIndexRemainsSearchableAfterCloseAndOpen() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 3);

        ingestDocumentsAndForceMergeForSingleShard(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f))
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );
        assertEquals(3, getHitCount(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10)));

        assertEquals(
            RestStatus.OK,
            RestStatus.fromCode(
                client().performRequest(new Request("POST", "/" + TEST_INDEX_NAME + "/_close")).getStatusLine().getStatusCode()
            )
        );
        assertEquals(
            RestStatus.OK,
            RestStatus.fromCode(
                client().performRequest(new Request("POST", "/" + TEST_INDEX_NAME + "/_open")).getStatusLine().getStatusCode()
            )
        );
        waitForClusterHealthGreen(String.valueOf(getNodeCount()));

        assertEquals(3, getDocCount(TEST_INDEX_NAME));
        assertEquals(3, getHitCount(search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10)));
    }

    @SuppressWarnings("unchecked")
    public void testSearchNestedFieldWithModelInferencingAndEmptyString() throws Exception {
        String modelId = prepareSparseEncodingModel();
        String sparseFieldName = "title_sparse"; // configured in SparseEncodingPipelineConfiguration.json
        createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING, 2);
        createSparseIndex(TEST_INDEX_NAME, sparseFieldName, 4, 0.4f, 0.5f, 1);

        String payload = prepareSparseBulkIngestPayload(TEST_INDEX_NAME, "title", null, List.of(), List.of(""), 1);
        bulkIngest(payload, PIPELINE_NAME);
        forceMerge(TEST_INDEX_NAME);
        waitForSegmentMerge(TEST_INDEX_NAME);

        assertEquals(1, getDocCount(TEST_INDEX_NAME));

        Map<String, Object> document = getDocById(TEST_INDEX_NAME, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        assertEquals("", source.get("title"));

        assertFalse("Sparse encoding field should not exist for empty string", source.containsKey(sparseFieldName));
    }
}
