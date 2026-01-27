/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.KnnQuery;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.services.SearchServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        logger.info("gRPC channel created, state: {}", channel.getState(true));
        return channel;
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

    // ===========================================================================================
    // SEARCH EXECUTION
    // ===========================================================================================

    public static SearchResponse executeSearch(ManagedChannel channel, SearchRequest request) {
        return executeSearch(channel, request, DEFAULT_TIMEOUT_SECONDS);
    }

    public static SearchResponse executeSearch(ManagedChannel channel, SearchRequest request, int timeoutSeconds) {
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
        return stub.search(request);
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
