/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.FieldCollapse;
import org.opensearch.protobufs.FieldSort;
import org.opensearch.protobufs.FieldSortMap;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.KnnQuery;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.services.SearchServiceGrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.experimental.UtilityClass;

/**
 * Helper class providing common gRPC testing utilities for neural-search integration tests.
 * This class can be reused across different gRPC integration tests for neural queries,
 * hybrid queries, and other query types.
 */
@UtilityClass
public class GrpcTestHelper {

    private static final Logger logger = LogManager.getLogger(GrpcTestHelper.class);

    public static final String DEFAULT_GRPC_HOST = "127.0.0.1";
    public static final int DEFAULT_GRPC_PORT = 9400;
    public static final int DEFAULT_TIMEOUT_SECONDS = 30;

    /**
     * Check if gRPC transport is expected to be available.
     * Returns true when either:
     * - tests.grpc.port is explicitly set (Gradle-managed cluster discovered the port), or
     * - tests.rest.cluster is NOT set (local testClusters run where gRPC is configured in build.gradle)
     *
     * Returns false when tests.rest.cluster is set but tests.grpc.port is not,
     * indicating an external cluster that likely doesn't have gRPC transport configured.
     *
     * Use with {@code assumeTrue(GrpcTestHelper.isGrpcTransportConfigured())} in test setUp
     * to gracefully skip gRPC tests in distribution integration tests (opensearch-build test.sh).
     */
    public static boolean isGrpcTransportConfigured() {
        boolean grpcPortExplicitlySet = System.getProperty("tests.grpc.port") != null;
        boolean externalCluster = System.getProperty("tests.rest.cluster") != null;
        if (grpcPortExplicitlySet) {
            logger.info("gRPC port explicitly configured: {}", getGrpcPort());
            return true;
        }
        if (externalCluster) {
            logger.info(
                "External cluster detected (tests.rest.cluster={}) without tests.grpc.port — gRPC tests will be skipped",
                System.getProperty("tests.rest.cluster")
            );
            return false;
        }
        // Local testClusters run — gRPC is configured in build.gradle
        logger.info("Local test cluster run — gRPC transport expected to be available");
        return true;
    }

    // ===========================================================================================
    // CHANNEL MANAGEMENT
    // ===========================================================================================

    public static String getGrpcHost() {
        return System.getProperty("tests.grpc.host", DEFAULT_GRPC_HOST);
    }

    public static int getGrpcPort() {
        return Integer.parseInt(System.getProperty("tests.grpc.port", String.valueOf(DEFAULT_GRPC_PORT)));
    }

    public static ManagedChannel createGrpcChannel() {
        return createGrpcChannel(getGrpcHost(), getGrpcPort());
    }

    public static ManagedChannel createGrpcChannel(String host, int port) {
        String target = host + ":" + port;
        logger.info("Creating gRPC channel to target: {}", target);
        try {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            logger.info("gRPC channel created, state: {}", channel.getState(true));
            return channel;
        } catch (Exception e) {
            logger.error("Failed to create gRPC channel to target: {}. Error: {}", target, e.getMessage(), e);
            throw new RuntimeException("Failed to create gRPC channel to " + target, e);
        }
    }

    public static void shutdownChannel(ManagedChannel channel, int timeoutSeconds) throws InterruptedException {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            channel.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
        }
    }

    public static void shutdownChannel(ManagedChannel channel) throws InterruptedException {
        shutdownChannel(channel, 5);
    }

    // ===========================================================================================
    // SEARCH REQUEST BUILDERS
    // ===========================================================================================

    public static SearchRequest buildSearchRequest(String index, QueryContainer query) {
        return buildSearchRequest(index, query, 10);
    }

    public static SearchRequest buildSearchRequest(String index, QueryContainer query, int size) {
        return SearchRequest.newBuilder()
            .addIndex(index)
            .setSearchRequestBody(SearchRequestBody.newBuilder().setQuery(query).setSize(size).build())
            .build();
    }

    public static SearchRequest buildSearchRequestWithPipeline(String index, QueryContainer query, int size, String pipeline) {
        return SearchRequest.newBuilder()
            .addIndex(index)
            .setSearchRequestBody(SearchRequestBody.newBuilder().setQuery(query).setSize(size).setSearchPipeline(pipeline).build())
            .build();
    }

    public static SearchRequest buildSearchRequestWithSort(
        String index,
        QueryContainer query,
        int size,
        String sortField,
        SortOrder sortOrder
    ) {
        SortCombinations sort = SortCombinations.newBuilder()
            .setFieldWithOrder(
                FieldSortMap.newBuilder().putFieldSortMap(sortField, FieldSort.newBuilder().setOrder(sortOrder).build()).build()
            )
            .build();
        return SearchRequest.newBuilder()
            .addIndex(index)
            .setSearchRequestBody(SearchRequestBody.newBuilder().setQuery(query).setSize(size).addSort(sort).build())
            .build();
    }

    public static SearchRequest buildSearchRequestWithCollapse(String index, QueryContainer query, int size, String collapseField) {
        return SearchRequest.newBuilder()
            .addIndex(index)
            .setSearchRequestBody(
                SearchRequestBody.newBuilder()
                    .setQuery(query)
                    .setSize(size)
                    .setCollapse(FieldCollapse.newBuilder().setField(collapseField).build())
                    .build()
            )
            .build();
    }

    // ===========================================================================================
    // SEARCH EXECUTION
    // ===========================================================================================

    public static SearchResponse executeSearch(ManagedChannel channel, SearchRequest request) {
        return executeSearch(channel, request, DEFAULT_TIMEOUT_SECONDS);
    }

    public static SearchResponse executeSearch(ManagedChannel channel, SearchRequest request, int timeoutSeconds) {
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);

        if (isSecurityEnabled()) {
            stub = addBasicAuthentication(stub);
        }

        return stub.search(request);
    }

    private static boolean isSecurityEnabled() {
        return "true".equals(System.getProperty("security.enabled"));
    }

    private static SearchServiceGrpc.SearchServiceBlockingStub addBasicAuthentication(SearchServiceGrpc.SearchServiceBlockingStub stub) {
        String username = System.getProperty("user", "admin");
        String password = System.getProperty("password", "admin");
        String base64Credentials = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), "Basic " + base64Credentials);

        logger.debug("Adding Basic Auth for gRPC: user={}", username);

        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    /**
     * Execute a search with retry logic to handle transient failures and reduce test flakiness.
     *
     * @param channel the gRPC channel
     * @param request the search request
     * @param timeoutSeconds timeout for each attempt
     * @param maxRetries maximum number of retry attempts
     * @param retryDelayMs delay between retries in milliseconds
     * @return the search response
     * @throws RuntimeException if all retry attempts fail
     */
    public static SearchResponse executeSearchWithRetry(
        ManagedChannel channel,
        SearchRequest request,
        int timeoutSeconds,
        int maxRetries,
        long retryDelayMs
    ) {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    logger.info("Retry attempt {} of {} for search request", attempt, maxRetries);
                    Thread.sleep(retryDelayMs);
                }
                return executeSearch(channel, request, timeoutSeconds);
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                    lastException = e;
                    logger.warn("Search attempt {} failed with transient error {}: {}", attempt + 1, code, e.getMessage());
                    if (attempt == maxRetries) {
                        logger.error("All {} retry attempts exhausted for search request", maxRetries + 1);
                    }
                } else {
                    throw e;
                }
            } catch (Exception e) {
                lastException = e;
                logger.warn("Search attempt {} failed: {}", attempt + 1, e.getMessage());
                if (attempt == maxRetries) {
                    logger.error("All {} retry attempts exhausted for search request", maxRetries + 1);
                }
            }
        }
        throw new RuntimeException("Search failed after " + (maxRetries + 1) + " attempts", lastException);
    }

    /**
     * Execute a search with default retry settings (3 retries, 1 second delay).
     */
    public static SearchResponse executeSearchWithRetry(ManagedChannel channel, SearchRequest request) {
        return executeSearchWithRetry(channel, request, DEFAULT_TIMEOUT_SECONDS, 3, 1000);
    }

    // ===========================================================================================
    // QUERY CONTAINER BUILDERS - Standard Query Types
    // ===========================================================================================

    public static QueryContainer createTermQueryContainer(String field, String value) {
        TermQuery termQuery = TermQuery.newBuilder().setField(field).setValue(FieldValue.newBuilder().setString(value).build()).build();
        return QueryContainer.newBuilder().setTerm(termQuery).build();
    }

    public static QueryContainer createMatchQueryContainer(String field, String query) {
        MatchQuery matchQuery = MatchQuery.newBuilder().setField(field).setQuery(FieldValue.newBuilder().setString(query).build()).build();
        return QueryContainer.newBuilder().setMatch(matchQuery).build();
    }

    public static QueryContainer createMatchAllQueryContainer() {
        return QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();
    }

    public static QueryContainer createBoolQueryContainer(QueryContainer... mustClauses) {
        BoolQuery.Builder boolBuilder = BoolQuery.newBuilder();
        for (QueryContainer clause : mustClauses) {
            boolBuilder.addMust(clause);
        }
        return QueryContainer.newBuilder().setBool(boolBuilder.build()).build();
    }

    // ===========================================================================================
    // QUERY CONTAINER BUILDERS - Neural/Vector Query Types
    // ===========================================================================================

    public static QueryContainer createKnnQueryContainer(String field, float[] vector, int k) {
        KnnQuery.Builder knnBuilder = KnnQuery.newBuilder().setField(field).setK(k);
        for (float v : vector) {
            knnBuilder.addVector(v);
        }
        return QueryContainer.newBuilder().setKnn(knnBuilder.build()).build();
    }

    public static QueryContainer createKnnQueryContainer(String field, float[] vector, int k, QueryContainer filter) {
        KnnQuery.Builder knnBuilder = KnnQuery.newBuilder().setField(field).setK(k).setFilter(filter);
        for (float v : vector) {
            knnBuilder.addVector(v);
        }
        return QueryContainer.newBuilder().setKnn(knnBuilder.build()).build();
    }

    public static QueryContainer createKnnQueryContainerWithMinScore(String field, float[] vector, float minScore) {
        KnnQuery.Builder knnBuilder = KnnQuery.newBuilder().setField(field).setMinScore(minScore);
        for (float v : vector) {
            knnBuilder.addVector(v);
        }
        return QueryContainer.newBuilder().setKnn(knnBuilder.build()).build();
    }
}
