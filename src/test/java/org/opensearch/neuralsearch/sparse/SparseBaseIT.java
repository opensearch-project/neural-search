/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
        createSparseIndex(indexName, fieldName, nPostings, alpha, clusterRatio, approximateThreshold, 1, 0);
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
        Request request = configureSparseIndex(
            indexName,
            fieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            shards,
            replicas
        );
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private Request configureSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        int shards,
        int replicas
    ) throws IOException {
        String indexSettings = prepareIndexSettings(shards, replicas);
        String indexMappings = prepareIndexMapping(nPostings, alpha, clusterRatio, approximateThreshold, fieldName);
        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        return request;
    }

    protected String convertTokensToText(Map<String, Float> tokens) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Float> entry : tokens.entrySet()) {
            if (!first) {
                builder.append(",");
            }
            builder.append("\"");
            builder.append(entry.getKey());
            builder.append("\": ");
            builder.append(entry.getValue());
            first = false;
        }
        return builder.toString();
    }

    protected String prepareIndexSettings() throws IOException {
        return prepareIndexSettings(1, 0);
    }

    protected String prepareIndexSettings(int shards, int replicas) throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("index")
            .field("number_of_routing_shards", shards) // Shard routing setting
            .field("sparse", true)
            .field("number_of_shards", shards)
            .field("number_of_replicas", replicas)
            .endObject()
            .endObject();
        return settingBuilder.toString();
    }

    protected void forceMerge(String indexName) throws IOException, ParseException {
        Request request = new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1");
        Response response = client().performRequest(request);
        String str = EntityUtils.toString(response.getEntity());
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected String prepareIndexMapping(int nPostings, float alpha, float clusterRatio, int approximateThreshold, String sparseFieldName)
        throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(sparseFieldName)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME)
            .startObject("parameters")
            .field("n_postings", nPostings) // Integer: length of posting list
            .field("summary_prune_ratio", alpha) // Float: alpha-prune ration for summary
            .field("cluster_ratio", clusterRatio) // Float: cluster ratio
            .field("approximate_threshold", approximateThreshold)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder.toString();
    }

    @SneakyThrows
    protected void prepareMultiShardReplicasIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        int shards = 3;
        int docCount = 100;
        // effective number of replica is capped by the number of OpenSearch nodes minus 1
        int replicas = Math.min(3, getNodeCount() - 1);
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount, shards, replicas);
        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));
        // Ingest documents
        List<Map<String, Float>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
        }

        List<String> routingIds = generateUniqueRoutingIds(shards);
        for (int i = 0; i < shards; ++i) {
            ingestDocuments(
                TEST_INDEX_NAME,
                TEST_TEXT_FIELD_NAME,
                TEST_SPARSE_FIELD_NAME,
                docs,
                Collections.emptyList(),
                i * docCount + 1,
                routingIds.get(i)
            );
        }

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME, shards, replicas);
        assertEquals(shards * (replicas + 1), getSegmentCount(TEST_INDEX_NAME));
    }

    @SneakyThrows
    protected void prepareMixSeismicRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 4);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            null,
            4
        );
    }

    @SneakyThrows
    protected void prepareOnlyRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 4);

        ingestDocumentsAndForceMerge(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f))
        );
    }

    protected void waitForSegmentMerge(String index) throws InterruptedException {
        waitForSegmentMerge(index, 1, 0);
    }

    protected void waitForSegmentMerge(String index, int shards, int replicas) throws InterruptedException {
        int maxRetry = 30;
        for (int i = 0; i < maxRetry; ++i) {
            if (shards * (1 + replicas) == getSegmentCount(index)) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    protected int getSegmentCount(String index) {
        Request request = new Request("GET", "/_cat/segments/" + index);
        try {
            Response response = client().performRequest(request);
            String str = EntityUtils.toString(response.getEntity());
            String[] lines = str.split("\n");
            return lines.length;
        } catch (IOException | ParseException e) {
            return 0;
        }
    }

    protected int getNodeCount() throws Exception {
        Request request = new Request("GET", "/_cat/nodes/");
        Response response = client().performRequest(request);
        String str = EntityUtils.toString(response.getEntity());
        String[] lines = str.split("\n");
        return lines.length;
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMerge(String index, String textField, String sparseField, List<Map<String, Float>> docTokens) {
        ingestDocumentsAndForceMerge(index, textField, sparseField, docTokens, null);
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMerge(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts
    ) {
        ingestDocuments(index, textField, sparseField, docTokens, docTexts, 1);

        forceMerge(index);
        // wait until force merge complete
        waitForSegmentMerge(index);
        assertEquals(1, getSegmentCount(index));
    }

    protected void ingestDocuments(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> text,
        int startingId
    ) {
        ingestDocuments(index, textField, sparseField, docTokens, text, startingId, null);
    }

    protected String prepareSparseBulkIngestPayload(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts,
        int startingId
    ) {
        StringBuilder payloadBuilder = new StringBuilder();
        int size = (StringUtils.isEmpty(sparseField) && docTokens.isEmpty()) ? docTexts.size() : docTokens.size();
        for (int i = 0; i < size; i++) {
            payloadBuilder.append(
                String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", index, startingId + i)
            );
            payloadBuilder.append(System.lineSeparator());
            String text = CollectionUtils.isEmpty(docTexts) ? "text" : docTexts.get(i);
            if (StringUtils.isEmpty(sparseField)) {
                payloadBuilder.append(String.format(Locale.ROOT, "{\"%s\": \"%s\"}", textField, text));
            } else {
                Map<String, Float> docToken = docTokens.get(i);
                String strTokens = convertTokensToText(docToken);
                payloadBuilder.append(
                    String.format(Locale.ROOT, "{\"%s\": \"%s\", \"%s\": {%s}}", textField, text, sparseField, strTokens)
                );
            }
            payloadBuilder.append(System.lineSeparator());
        }
        return payloadBuilder.toString();
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
        String payload = prepareSparseBulkIngestPayload(index, textField, sparseField, docTokens, docTexts, startingId);
        bulkIngest(payload, null, routing);
    }

    /**
     * Generates unique routing IDs that map to different shards for multi-shard testing.
     * Uses Murmur3HashFunction to simulate OpenSearch's shard routing algorithm, which determines
     * which shard a document belongs to based on its routing value hash.
     *
     * The method iterates through candidate routing values and uses the same hash function
     * that OpenSearch uses internally to ensure documents are distributed across different shards.
     *
     * @param num number of routing ids, should be less than or equal to shard number.
     * @return a list of routing ids that will route documents to different shards
     */
    protected List<String> generateUniqueRoutingIds(int num) {
        List<String> routingIds = new ArrayList<>();
        Set<Integer> uniqueShardIds = new HashSet<>();
        for (int i = 0; i < 10000; ++i) {
            String candidate = String.valueOf(i);
            int hash = Murmur3HashFunction.hash(candidate);
            int shardId = Math.floorMod(hash, num);
            if (uniqueShardIds.contains(shardId)) {
                continue;
            }
            uniqueShardIds.add(shardId);
            routingIds.add(candidate);
            if (routingIds.size() == num) {
                break;
            }
        }
        return routingIds;
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
}
