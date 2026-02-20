/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc;

import static org.opensearch.neuralsearch.grpc.GrpcTestHelper.buildSearchRequest;
import static org.opensearch.neuralsearch.grpc.GrpcTestHelper.createKnnQueryContainer;
import static org.opensearch.neuralsearch.grpc.GrpcTestHelper.createMatchAllQueryContainer;
import static org.opensearch.neuralsearch.grpc.GrpcTestHelper.createMatchQueryContainer;
import static org.opensearch.neuralsearch.grpc.GrpcTestHelper.createTermQueryContainer;

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.HybridQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;

/**
 * Integration test for gRPC Hybrid Query functionality.
 *
 * This test verifies that HybridQuery can be:
 * 1. Created in Protocol Buffer format
 * 2. Sent via gRPC to OpenSearch
 * 3. Executed correctly and return expected results
 *
 * Prerequisites:
 * - OpenSearch cluster must have gRPC transport plugin enabled
 * - The cluster should be running and accessible via gRPC (typically on port 9400)
 *
 * Note: This test creates protobuf queries directly since HybridQueryBuilderProtoConverter
 * only implements fromProto (protobuf -> QueryBuilder), not toProto (QueryBuilder -> protobuf).
 */
public class HybridQueryGrpcIT extends BaseNeuralSearchIT {

    private static final Logger logger = LogManager.getLogger(HybridQueryGrpcIT.class);

    private static final String TEST_INDEX_NAME = "test-grpc-hybrid-index";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_TITLE_FIELD_NAME = "title";
    private static final String TEST_STATUS_FIELD_NAME = "status";
    private static final String TEST_CATEGORY_FIELD_NAME = "category";
    private static final String TEST_VECTOR_FIELD_NAME = "embedding";
    private static final int TEST_VECTOR_DIMENSION = 3;

    private ManagedChannel grpcChannel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();

        // Set up test index with diverse test data
        initializeIndexIfNotExist(TEST_INDEX_NAME);

        // Create gRPC channel for tests using the shared helper
        grpcChannel = GrpcTestHelper.createGrpcChannel();
    }

    @After
    public void tearDownGrpc() throws Exception {
        // Shutdown gRPC channel
        GrpcTestHelper.shutdownChannel(grpcChannel);

        // Clean up test resources - delete test index
        deleteTestIndex();
    }

    /**
     * Delete the test index to clean up resources after test execution.
     */
    @SneakyThrows
    private void deleteTestIndex() {
        try {
            if (checkIndexExists(TEST_INDEX_NAME)) {
                logger.info("Deleting test index: {}", TEST_INDEX_NAME);
                Request deleteRequest = new Request("DELETE", "/" + TEST_INDEX_NAME);
                Response response = client().performRequest(deleteRequest);
                if (response.getStatusLine().getStatusCode() == 200) {
                    logger.info("Successfully deleted test index: {}", TEST_INDEX_NAME);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to delete test index {}: {}", TEST_INDEX_NAME, e.getMessage());
        }
    }

    // ===========================================================================================
    // BASIC FUNCTIONALITY TESTS - Verify end-to-end gRPC query execution
    // ===========================================================================================

    /**
     * Test gRPC connectivity with a simple MatchAll query.
     * This replaced the TCP connectivity test to avoid forbidden socket APIs.
     */
    @SneakyThrows
    public void testGrpcConnectivityWithMatchAllQuery() {
        String host = GrpcTestHelper.getGrpcHost();
        int port = GrpcTestHelper.getGrpcPort();
        logger.info("Testing gRPC connectivity to {}:{}", host, port);

        try {
            QueryContainer query = createMatchAllQueryContainer();
            SearchRequest request = buildSearchRequest(TEST_INDEX_NAME, query);

            SearchResponse response = GrpcTestHelper.executeSearch(grpcChannel, request, 10);

            assertNotNull("Search response should not be null", response);
            logger.info("gRPC connection successful to {}:{}", host, port);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to connect via gRPC to {}:{} - {}", host, port, e.getMessage());
            fail("Cannot connect to gRPC endpoint " + host + ":" + port + " - " + e.getMessage());
        }
    }

    /**
     * Test that a basic hybrid query executes via gRPC and returns results.
     * This validates the complete round-trip: client -> gRPC -> OpenSearch -> response
     */
    @SneakyThrows
    public void testBasicHybridQueryReturnsResults() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "search"))
            .addQueries(createMatchAllQueryContainer())
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery, 10);

        assertNotNull("Search response should not be null", response);
        assertTrue("Response should have hits", response.getHits().getHitsCount() > 0);
        // Verify total hits count
        assertTrue("Total hits should be greater than 0", response.getHits().getTotal().getTotalHits().getValue() > 0);
        assertEquals("Total relation should be EQUAL_TO", "EQUAL_TO", response.getHits().getTotal().getTotalHits().getRelation().name());
        logger.info(
            "Hybrid query returned {} hits, total: {}",
            response.getHits().getHitsCount(),
            response.getHits().getTotal().getTotalHits().getValue()
        );
    }

    /**
     * Test hybrid query with multiple term queries combines results correctly.
     * Validates that sub-queries are executed and results are merged.
     */
    @SneakyThrows
    public void testHybridQueryCombinesMultipleSubQueries() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "machine"))
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "vector"))
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should find documents matching either sub-query", response.getHits().getHitsCount() >= 2);
        // Verify total hits count
        assertTrue("Total hits should be at least 2", response.getHits().getTotal().getTotalHits().getValue() >= 2);
        logger.info(
            "Multiple sub-queries returned {} hits, total: {}",
            response.getHits().getHitsCount(),
            response.getHits().getTotal().getTotalHits().getValue()
        );
    }

    // ===========================================================================================
    // FILTER TESTS - Verify filter restricts results across all sub-queries
    // ===========================================================================================

    /**
     * Test that filter restricts hybrid query results.
     * Filter should apply to all sub-queries, not just one.
     */
    @SneakyThrows
    public void testHybridQueryWithFilterRestrictsResults() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchAllQueryContainer())
            .setFilter(createTermQueryContainer(TEST_STATUS_FIELD_NAME, "active"))
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should return results with filter", response.getHits().getHitsCount() > 0);
        // Based on test data, 2 documents have status=active (doc1, doc2)
        assertEquals("Should return exactly 2 active documents", 2, response.getHits().getTotal().getTotalHits().getValue());
        logger.info(
            "Filter test returned {} hits, total: {}",
            response.getHits().getHitsCount(),
            response.getHits().getTotal().getTotalHits().getValue()
        );
    }

    /**
     * Test filter with no matching documents returns empty results.
     */
    @SneakyThrows
    public void testHybridQueryWithFilterNoMatches() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchAllQueryContainer())
            .setFilter(createTermQueryContainer(TEST_STATUS_FIELD_NAME, "nonexistent"))
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertEquals("Should return no documents with non-matching filter", 0, response.getHits().getHitsCount());
        assertEquals("Total hits should be 0", 0, response.getHits().getTotal().getTotalHits().getValue());
    }

    /**
     * Test hybrid query with complex bool filter.
     */
    @SneakyThrows
    public void testHybridQueryWithBoolFilter() {
        BoolQuery boolFilter = BoolQuery.newBuilder()
            .addMust(createTermQueryContainer(TEST_STATUS_FIELD_NAME, "active"))
            .addMust(createTermQueryContainer(TEST_CATEGORY_FIELD_NAME, "tech"))
            .build();

        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchAllQueryContainer())
            .setFilter(QueryContainer.newBuilder().setBool(boolFilter).build())
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        logger.info("Bool filter test returned {} hits", response.getHits().getHitsCount());
    }

    // ===========================================================================================
    // OPTIONAL FIELDS TESTS - Verify paginationDepth, queryName, boost handling
    // ===========================================================================================

    /**
     * Test hybrid query with pagination depth setting.
     */
    @SneakyThrows
    public void testHybridQueryWithPaginationDepth() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createMatchAllQueryContainer()).setPaginationDepth(100).build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Query with pagination depth should execute successfully", response.getHits().getHitsCount() > 0);
    }

    /**
     * Test hybrid query with query name for debugging/profiling.
     */
    @SneakyThrows
    public void testHybridQueryWithQueryName() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchAllQueryContainer())
            .setXName("my-hybrid-query-name")
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Query with name should execute successfully", response.getHits().getHitsCount() > 0);
    }

    /**
     * Test hybrid query with all optional fields combined.
     */
    @SneakyThrows
    public void testHybridQueryWithAllOptionalFields() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "search"))
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "learning"))
            .setFilter(createTermQueryContainer(TEST_STATUS_FIELD_NAME, "active"))
            .setPaginationDepth(50)
            .setXName("complete-hybrid-query")
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should return matching active documents", response.getHits().getHitsCount() > 0);
    }

    // ===========================================================================================
    // MIXED QUERY TYPES - Verify different sub-query types work together
    // ===========================================================================================

    /**
     * Test hybrid query mixing Term + Match + MatchAll queries.
     */
    @SneakyThrows
    public void testHybridQueryWithMixedQueryTypes() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer(TEST_CATEGORY_FIELD_NAME, "tech"))
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "neural search"))
            .addQueries(createMatchAllQueryContainer())
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should return results", response.getHits().getHitsCount() > 0);
        logger.info("Mixed query types test returned {} hits", response.getHits().getHitsCount());
    }

    /**
     * Test hybrid query with maximum allowed sub-queries (5).
     */
    @SneakyThrows
    public void testHybridQueryWithMaxSubQueries() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "search"))
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "learning"))
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "neural"))
            .addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "vector"))
            .addQueries(createMatchAllQueryContainer())
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should execute with max sub-queries", response.getHits().getHitsCount() > 0);
    }

    /**
     * Test hybrid query with lexical (Match) + neural/vector (KNN) sub-queries.
     * This demonstrates combining traditional text search with vector similarity search.
     */
    @SneakyThrows
    public void testHybridQueryWithLexicalAndKnnSubQueries() {
        // Query vector close to doc1's embedding [0.1, 0.2, 0.3]
        float[] queryVector = new float[] { 0.15f, 0.25f, 0.35f };

        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "neural search"))
            .addQueries(createKnnQueryContainer(TEST_VECTOR_FIELD_NAME, queryVector, 3))
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        assertTrue("Should return results from both lexical and KNN queries", response.getHits().getHitsCount() > 0);
        logger.info("Lexical + KNN hybrid query returned {} hits", response.getHits().getHitsCount());
    }

    /**
     * Test hybrid query with lexical + KNN + filter to demonstrate full hybrid search capability.
     */
    @SneakyThrows
    public void testHybridQueryWithLexicalKnnAndFilter() {
        float[] queryVector = new float[] { 0.4f, 0.5f, 0.6f };

        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createMatchQueryContainer(TEST_TEXT_FIELD_NAME, "search"))
            .addQueries(createKnnQueryContainer(TEST_VECTOR_FIELD_NAME, queryVector, 4))
            .setFilter(createTermQueryContainer(TEST_STATUS_FIELD_NAME, "active"))
            .build();

        SearchResponse response = executeHybridSearch(hybridQuery);

        assertNotNull("Search response should not be null", response);
        logger.info("Lexical + KNN + filter hybrid query returned {} hits", response.getHits().getHitsCount());
    }

    // ===========================================================================================
    // ERROR HANDLING TESTS - Verify proper gRPC error codes for invalid requests
    // ===========================================================================================

    /**
     * Test that exceeding max sub-queries returns proper gRPC error.
     */
    @SneakyThrows
    public void testErrorExceedsMaxSubQueries() {
        HybridQuery.Builder builder = HybridQuery.newBuilder();
        for (int i = 0; i < 6; i++) {
            builder.addQueries(createTermQueryContainer(TEST_TEXT_FIELD_NAME, "term" + i));
        }

        StatusRuntimeException exception = expectThrows(StatusRuntimeException.class, () -> executeHybridSearch(builder.build()));

        assertTrue(
            "Error should indicate max sub-queries exceeded",
            exception.getMessage().contains("Number of sub-queries exceeds maximum")
                || exception.getStatus().getCode().name().equals("INVALID_ARGUMENT")
        );
    }

    /**
     * Test that empty queries returns proper gRPC error.
     */
    @SneakyThrows
    public void testErrorEmptyQueries() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().build();

        StatusRuntimeException exception = expectThrows(StatusRuntimeException.class, () -> executeHybridSearch(hybridQuery));

        assertTrue(
            "Error should indicate queries required",
            exception.getMessage().contains("requires 'queries' field") || exception.getStatus().getCode().name().equals("INVALID_ARGUMENT")
        );
    }

    /**
     * Test that non-default boost returns proper gRPC error.
     */
    @SneakyThrows
    public void testErrorNonDefaultBoost() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createMatchAllQueryContainer()).setBoost(2.0f).build();

        StatusRuntimeException exception = expectThrows(StatusRuntimeException.class, () -> executeHybridSearch(hybridQuery));

        assertTrue(
            "Error should indicate boost not supported",
            exception.getMessage().contains("does not support") || exception.getStatus().getCode().name().equals("INVALID_ARGUMENT")
        );
    }

    /**
     * Test search on non-existent index returns proper gRPC error.
     */
    @SneakyThrows
    public void testErrorNonExistentIndex() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createMatchAllQueryContainer()).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();
        SearchRequest request = buildSearchRequest("non-existent-index-12345", queryContainer);

        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(grpcChannel);

        StatusRuntimeException exception = expectThrows(StatusRuntimeException.class, () -> stub.search(request));

        assertNotNull("Should throw exception for non-existent index", exception);
    }

    // ===========================================================================================
    // HELPER METHODS
    // ===========================================================================================

    /**
     * Execute a hybrid search using the shared gRPC channel with retry logic.
     */
    private SearchResponse executeHybridSearch(HybridQuery hybridQuery) {
        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();
        SearchRequest request = GrpcTestHelper.buildSearchRequest(TEST_INDEX_NAME, queryContainer);
        return GrpcTestHelper.executeSearchWithRetry(grpcChannel, request);
    }

    /**
     * Execute a hybrid search with custom size and retry logic.
     */
    private SearchResponse executeHybridSearch(HybridQuery hybridQuery, int size) {
        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();
        SearchRequest request = GrpcTestHelper.buildSearchRequest(TEST_INDEX_NAME, queryContainer, size);
        return GrpcTestHelper.executeSearchWithRetry(grpcChannel, request);
    }

    // ===========================================================================================
    // INDEX AND DATA SETUP
    // ===========================================================================================

    /**
     * Initialize test index with mappings and diverse test documents.
     *
     * Test Documents:
     * - doc1: text="neural search for machine learning", status=active, category=tech, embedding=[0.1, 0.2, 0.3]
     * - doc2: text="semantic search with transformers", status=active, category=science, embedding=[0.4, 0.5, 0.6]
     * - doc3: text="vector database for embeddings", status=inactive, category=tech, embedding=[0.7, 0.8, 0.9]
     * - doc4: text="traditional keyword search methods", status=inactive, category=education, embedding=[0.2, 0.3, 0.4]
     */
    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        if (checkIndexExists(indexName)) {
            return;
        }

        String indexConfiguration = String.format(
            Locale.ROOT,
            "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,\"index.knn\":true},"
                + "\"mappings\":{\"properties\":{"
                + "\"%s\":{\"type\":\"text\",\"analyzer\":\"standard\"},"
                + "\"%s\":{\"type\":\"text\"},"
                + "\"%s\":{\"type\":\"keyword\"},"
                + "\"%s\":{\"type\":\"keyword\"},"
                + "\"%s\":{\"type\":\"knn_vector\",\"dimension\":%d,\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}"
                + "}}}",
            TEST_TEXT_FIELD_NAME,
            TEST_TITLE_FIELD_NAME,
            TEST_STATUS_FIELD_NAME,
            TEST_CATEGORY_FIELD_NAME,
            TEST_VECTOR_FIELD_NAME,
            TEST_VECTOR_DIMENSION
        );

        createIndex(indexName, indexConfiguration);
        indexTestDocuments(indexName);
    }

    /**
     * Check if index exists
     */
    @SneakyThrows
    private boolean checkIndexExists(String indexName) {
        try {
            Response response = client().performRequest(new Request("HEAD", "/" + indexName));
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Index diverse test documents for comprehensive testing.
     */
    @SneakyThrows
    private void indexTestDocuments(String indexName) {
        String doc1 = String.format(
            Locale.ROOT,
            "{\"%s\":\"neural search for machine learning applications\",\"%s\":\"ML Guide\",\"%s\":\"active\",\"%s\":\"tech\",\"%s\":[0.1,0.2,0.3]}",
            TEST_TEXT_FIELD_NAME,
            TEST_TITLE_FIELD_NAME,
            TEST_STATUS_FIELD_NAME,
            TEST_CATEGORY_FIELD_NAME,
            TEST_VECTOR_FIELD_NAME
        );

        String doc2 = String.format(
            Locale.ROOT,
            "{\"%s\":\"semantic search with transformers and embeddings\",\"%s\":\"NLP Tutorial\",\"%s\":\"active\",\"%s\":\"science\",\"%s\":[0.4,0.5,0.6]}",
            TEST_TEXT_FIELD_NAME,
            TEST_TITLE_FIELD_NAME,
            TEST_STATUS_FIELD_NAME,
            TEST_CATEGORY_FIELD_NAME,
            TEST_VECTOR_FIELD_NAME
        );

        String doc3 = String.format(
            Locale.ROOT,
            "{\"%s\":\"vector database for storing embeddings efficiently\",\"%s\":\"DB Overview\",\"%s\":\"inactive\",\"%s\":\"tech\",\"%s\":[0.7,0.8,0.9]}",
            TEST_TEXT_FIELD_NAME,
            TEST_TITLE_FIELD_NAME,
            TEST_STATUS_FIELD_NAME,
            TEST_CATEGORY_FIELD_NAME,
            TEST_VECTOR_FIELD_NAME
        );

        String doc4 = String.format(
            Locale.ROOT,
            "{\"%s\":\"traditional keyword search methods and techniques\",\"%s\":\"Search Basics\",\"%s\":\"inactive\",\"%s\":\"education\",\"%s\":[0.2,0.3,0.4]}",
            TEST_TEXT_FIELD_NAME,
            TEST_TITLE_FIELD_NAME,
            TEST_STATUS_FIELD_NAME,
            TEST_CATEGORY_FIELD_NAME,
            TEST_VECTOR_FIELD_NAME
        );

        ingestDocument(indexName, doc1, "1");
        ingestDocument(indexName, doc2, "2");
        ingestDocument(indexName, doc3, "3");
        ingestDocument(indexName, doc4, "4");

        // Refresh to make documents searchable
        client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
