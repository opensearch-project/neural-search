/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.SparseTestCommon;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_INGEST;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_SEARCH;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

/**
 * Base Integration tests for seismic feature
 */
public abstract class SparseBaseIT extends BaseNeuralSearchIT {

    protected static final String ALGO_NAME = SparseConstants.SEISMIC;
    protected static final String SPARSE_MEMORY_USAGE_METRIC_NAME = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString();
    protected static final String SPARSE_MEMORY_USAGE_METRIC_PATH = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getFullPath();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    protected void createSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        SparseTestCommon.createSparseIndex(client(), indexName, fieldName, nPostings, alpha, clusterRatio, approximateThreshold);
    }

    protected void createSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        int shards,
        int replicas
    ) throws IOException {
        SparseTestCommon.createSparseIndex(
            client(),
            indexName,
            fieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            shards,
            replicas
        );
    }

    protected String prepareIndexSettings() throws IOException {
        return SparseTestCommon.prepareIndexSettings(1, 0);
    }

    protected String prepareIndexSettings(int shards, int replicas) throws IOException {
        return SparseTestCommon.prepareIndexSettings(shards, replicas);
    }

    protected void forceMerge(String indexName) throws IOException, ParseException {
        SparseTestCommon.forceMerge(client(), indexName);
    }

    protected String prepareIndexMapping(int nPostings, float alpha, float clusterRatio, int approximateThreshold, String sparseFieldName)
        throws IOException {
        return SparseTestCommon.prepareIndexMapping(nPostings, alpha, clusterRatio, approximateThreshold, sparseFieldName);
    }

    protected String prepareIndexMapping(
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        float quantizationCeilingIngest,
        float quantizationCeilingSearch,
        String sparseFieldName
    ) throws IOException {
        return SparseTestCommon.prepareIndexMapping(
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            quantizationCeilingIngest,
            quantizationCeilingSearch,
            sparseFieldName
        );
    }

    @SneakyThrows
    protected List<Map<String, Float>> prepareIngestDocuments(int docCount) {
        return SparseTestCommon.prepareIngestDocuments(docCount);
    }

    @SneakyThrows
    protected void prepareSparseIndex(String index, String sparseField, String textField) {
        SparseTestCommon.prepareSparseIndex(client(), index, sparseField, textField);
    }

    @SneakyThrows
    protected void prepareMultiShardReplicasIndex(String index, String sparseField, String textField, int shards, int replicas) {
        SparseTestCommon.prepareMultiShardReplicasIndex(client(), index, sparseField, textField, shards, replicas);
    }

    @SneakyThrows
    protected void prepareNonSparseIndex(String index) {
        SparseTestCommon.prepareNonSparseIndex(client(), index);
    }

    @SneakyThrows
    protected void prepareMixSeismicRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        SparseTestCommon.prepareMixSeismicRankFeaturesIndex(client(), TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
    }

    @SneakyThrows
    protected void prepareOnlyRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        SparseTestCommon.prepareOnlyRankFeaturesIndex(client(), TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
    }

    @SneakyThrows
    protected void createIndexWithMultipleSeismicFields(String indexName, List<String> fieldNames) {
        SparseTestCommon.createIndexWithMultipleSeismicFields(client(), indexName, fieldNames);
    }

    protected void waitForSegmentMerge(String index) throws InterruptedException {
        SparseTestCommon.waitForSegmentMerge(client(), index);
    }

    protected void waitForSegmentMerge(String index, int shards, int replicas) throws InterruptedException {
        SparseTestCommon.waitForSegmentMerge(client(), index, shards, replicas);
    }

    protected int getSegmentCount(String index) {
        return SparseTestCommon.getSegmentCount(client(), index);
    }

    protected int getNodeCount() throws Exception {
        return SparseTestCommon.getNodeCount(client());
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMergeForSingleShard(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens
    ) {
        SparseTestCommon.ingestDocumentsAndForceMergeForSingleShard(client(), index, textField, sparseField, docTokens);
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMergeForSingleShard(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts
    ) {
        SparseTestCommon.ingestDocumentsAndForceMergeForSingleShard(client(), index, textField, sparseField, docTokens, docTexts);
    }

    protected void ingestDocuments(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> text,
        int startingId
    ) {
        SparseTestCommon.ingestDocuments(index, textField, sparseField, docTokens, text, startingId);
    }

    protected String prepareSparseBulkIngestPayload(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts,
        int startingId
    ) {
        return SparseTestCommon.prepareSparseBulkIngestPayload(index, textField, sparseField, docTokens, docTexts, startingId);
    }

    protected void ingestDocuments(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts,
        int startingId,
        String routing
    ) {
        SparseTestCommon.ingestDocuments(index, textField, sparseField, docTokens, docTexts, startingId, routing);
    }

    @SneakyThrows
    protected List<Double> getSparseMemoryUsageStatsAcrossNodes() {
        Request request = new Request("GET", NeuralSearch.NEURAL_BASE_URI + "/stats/" + SPARSE_MEMORY_USAGE_METRIC_NAME);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> nodeStatsResponseList = parseNodeStatsResponse(responseBody);

        List<Double> sparseMemoryUsageStats = new ArrayList<>();
        for (Map<String, Object> nodeStatsResponse : nodeStatsResponseList) {
            String stringValue = getNestedValue(nodeStatsResponse, SPARSE_MEMORY_USAGE_METRIC_PATH).toString();
            sparseMemoryUsageStats.add(NumberUtils.createDouble(stringValue));
        }
        return sparseMemoryUsageStats;
    }

    protected NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(String field, int cut, float hf, int k, Map<String, Float> query) {
        return SparseTestCommon.getNeuralSparseQueryBuilder(field, cut, hf, k, query);
    }

    protected NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(
        String field,
        int cut,
        float hf,
        int k,
        Map<String, Float> query,
        QueryBuilder filter
    ) {
        return SparseTestCommon.getNeuralSparseQueryBuilder(field, cut, hf, k, query, filter);
    }

    @SneakyThrows
    protected int getEffectiveReplicaCount(int replicas) {
        return SparseTestCommon.getEffectiveReplicaCount(client(), replicas);
    }

    protected List<String> getDocIDs(Map<String, Object> searchResults) {
        return SparseTestCommon.getDocIDs(searchResults);
    }

    protected void updateSparseVector(String index, String docId, String field, Map<String, Float> docTokens) throws IOException {
        SparseTestCommon.updateSparseVector(client(), index, docId, field, docTokens);
    }
}
