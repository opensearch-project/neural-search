/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
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
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base Integration tests for seismic feature.
 *
 * <p>Subclasses are parameterized over {@link SparseEngine}: every sparse index this class creates
 * is configured with {@link #engine}, so a single test body runs once per engine. A subclass opts in
 * by declaring a {@link ParametersFactory} method returning {@link #allEngines()} (or
 * {@link #luceneEngineOnly()} when the feature under test has no native counterpart) and forwarding
 * the engine through its constructor.
 */
public abstract class SparseBaseIT extends BaseNeuralSearchIT {

    protected static final String ALGO_NAME = SparseConstants.SEISMIC;
    protected static final String SPARSE_MEMORY_USAGE_METRIC_NAME = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString();
    protected static final String SPARSE_MEMORY_USAGE_METRIC_PATH = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getFullPath();

    /**
     * The native engine indexes unquantized float32 values, so it neither clips weights to the
     * ceilings nor rescales scores by them.
     */
    protected static final String QUANTIZATION_IS_LUCENE_ONLY = "quantization_ceiling_* is inert on the native engine";

    /**
     * Engine under test. Injected into every sparse mapping created through this class.
     */
    protected final SparseEngine engine;

    public SparseBaseIT(SparseEngine engine) {
        this.engine = engine;
    }

    /**
     * Every engine, the default parameter set for sparse integration tests.
     */
    public static Collection<Object[]> allEngines() {
        return List.of(new Object[] { SparseEngine.LUCENE }, new Object[] { SparseEngine.NATIVE });
    }

    /**
     * Only the Lucene engine, for suites covering behavior the native engine does not implement.
     */
    public static Collection<Object[]> luceneEngineOnly() {
        return List.<Object[]>of(new Object[] { SparseEngine.LUCENE });
    }

    /**
     * Only the native engine, for suites covering behavior the Lucene engine does not implement.
     */
    public static Collection<Object[]> nativeEngineOnly() {
        return List.<Object[]>of(new Object[] { SparseEngine.NATIVE });
    }

    /**
     * Skips the running test unless the engine under test is Lucene. Use for assertions that are
     * only meaningful on the Lucene engine, e.g. quantized scores or rank_features fallback.
     */
    protected void assumeLuceneEngine(String reason) {
        assumeTrue(reason, SparseEngine.LUCENE == engine);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // The dynamic gate defaults to off, so every native suite has to open it. Harmless on the
        // Lucene engine, and set unconditionally so a NATIVE run never depends on test ordering.
        updateClusterSettings(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, true);
    }

    protected void createSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        SparseTestCommon.createSparseIndex(client(), engine, indexName, fieldName, nPostings, alpha, clusterRatio, approximateThreshold);
    }

    /** Single shard, no replica, with the forward index layout the native engine should build. */
    protected void createSparseIndex(
        String indexName,
        String fieldName,
        SparseForwardIndex forwardIndex,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        SparseTestCommon.createSparseIndex(
            client(),
            engine,
            forwardIndex,
            indexName,
            fieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold
        );
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
            engine,
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

    protected void createNestedSparseIndex(
        String indexName,
        String nestedFieldName,
        String sparseFieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        SparseTestCommon.createNestedSparseIndex(
            client(),
            engine,
            indexName,
            nestedFieldName,
            sparseFieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            1,
            0
        );
    }

    protected void createNestedSparseIndex(
        String indexName,
        String nestedFieldName,
        String sparseFieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        int shards,
        int replicas
    ) throws IOException {
        SparseTestCommon.createNestedSparseIndex(
            client(),
            engine,
            indexName,
            nestedFieldName,
            sparseFieldName,
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
        return SparseTestCommon.prepareIndexMapping(engine, nPostings, alpha, clusterRatio, approximateThreshold, sparseFieldName);
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
            engine,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            quantizationCeilingIngest,
            quantizationCeilingSearch,
            sparseFieldName
        );
    }

    protected String prepareMixedNestedFieldsIndexMapping(
        String sparseAnnParentField,
        String plainNeuralSparseParentField,
        String nestedChunkField,
        String sparseFieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        return SparseTestCommon.prepareMixedNestedFieldsIndexMapping(
            engine,
            sparseAnnParentField,
            plainNeuralSparseParentField,
            nestedChunkField,
            sparseFieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold
        );
    }

    protected String prepareMixedFieldTypeIndexMapping(
        String parentField,
        String rankFeaturesField,
        String sparseVectorField,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        return SparseTestCommon.prepareMixedFieldTypeIndexMapping(
            engine,
            parentField,
            rankFeaturesField,
            sparseVectorField,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold
        );
    }

    @SneakyThrows
    protected List<Map<String, Float>> prepareIngestDocuments(int docCount) {
        return SparseTestCommon.prepareIngestDocuments(docCount);
    }

    @SneakyThrows
    protected void prepareSparseIndex(String index, String sparseField, String textField) {
        SparseTestCommon.prepareSparseIndex(client(), engine, index, sparseField, textField);
    }

    @SneakyThrows
    protected void prepareMultiShardReplicasIndex(String index, String sparseField, String textField, int shards, int replicas) {
        SparseTestCommon.prepareMultiShardReplicasIndex(client(), engine, index, sparseField, textField, shards, replicas);
    }

    @SneakyThrows
    protected void prepareNonSparseIndex(String index) {
        SparseTestCommon.prepareNonSparseIndex(client(), index);
    }

    @SneakyThrows
    protected void prepareMixSeismicRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        SparseTestCommon.prepareMixSeismicRankFeaturesIndex(
            client(),
            engine,
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            TEST_TEXT_FIELD_NAME
        );
    }

    @SneakyThrows
    protected void prepareOnlyRankFeaturesIndex(String TEST_INDEX_NAME, String TEST_SPARSE_FIELD_NAME, String TEST_TEXT_FIELD_NAME) {
        SparseTestCommon.prepareOnlyRankFeaturesIndex(client(), engine, TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, TEST_TEXT_FIELD_NAME);
    }

    @SneakyThrows
    protected void createIndexWithMultipleSeismicFields(String indexName, List<String> fieldNames) {
        SparseTestCommon.createIndexWithMultipleSeismicFields(client(), engine, indexName, fieldNames);
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

    @SneakyThrows
    protected void ingestNestedDocumentsAndForceMergeForSingleShard(
        String index,
        String nestedFieldName,
        List<List<Map<String, Float>>> documentsWithChunks,
        String pipelineName
    ) {
        SparseTestCommon.ingestNestedDocumentsAndForceMergeForSingleShard(
            client(),
            index,
            nestedFieldName,
            documentsWithChunks,
            pipelineName
        );
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

    protected Map<String, Object> searchWithExplain(String index, QueryBuilder queryBuilder, int resultSize) {
        return search(index, queryBuilder, null, resultSize, Map.of("explain", "true"), null);
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

    @SuppressWarnings("unchecked")
    protected void assertExplanationContains(Map<String, Object> explanation, String... expectedDescriptions) {
        assertNotNull("Explanation should be present", explanation);

        List<Map<String, Object>> details = (List<Map<String, Object>>) explanation.get("details");
        assertNotNull("Explanation details should be present", details);
        assertFalse("Explanation should have details", details.isEmpty());

        Set<String> foundDescriptions = new HashSet<>();
        for (Map<String, Object> detail : details) {
            String detailDesc = (String) detail.get("description");
            for (String expected : expectedDescriptions) {
                if (detailDesc.contains(expected)) {
                    foundDescriptions.add(expected);
                }
            }
        }

        for (String expected : expectedDescriptions) {
            assertTrue("Explanation should contain: " + expected, foundDescriptions.contains(expected));
        }
    }

    /**
     * Asserts that the explanation reports the score the hit was actually ranked by. This is the
     * assertion that pins the explain arithmetic against the scorer's: explain recomputes the score
     * from the stored vector, so a formula that drifts from the scorer's (a dropped boost, a
     * quantization range applied at the wrong ceiling) shows up here and nowhere else.
     */
    @SuppressWarnings("unchecked")
    protected void assertExplanationScoreMatchesHit(Map<String, Object> hit) {
        Map<String, Object> explanation = (Map<String, Object>) hit.get("_explanation");
        assertNotNull("Explanation should be present", explanation);
        float hitScore = Float.parseFloat(hit.get("_score").toString());
        Object explanationValue = explanation.get("value");
        assertNotNull("Explanation should carry a value", explanationValue);
        float explainedScore = Float.parseFloat(explanationValue.toString());
        // Relative, since a score's magnitude depends on the quantization ceilings in play.
        assertEquals("Explained score should match the hit's score", hitScore, explainedScore, Math.max(1e-4f, Math.abs(hitScore) * 1e-3f));
    }

    @SuppressWarnings("unchecked")
    protected void assertExplanationNotContains(Map<String, Object> explanation, String... unexpectedDescriptions) {
        assertNotNull("Explanation should be present", explanation);

        List<Map<String, Object>> details = (List<Map<String, Object>>) explanation.get("details");
        assertNotNull("Explanation details should be present", details);

        for (Map<String, Object> detail : details) {
            String detailDesc = (String) detail.get("description");
            for (String unexpected : unexpectedDescriptions) {
                assertFalse("Explanation should NOT contain: " + unexpected, detailDesc.contains(unexpected));
            }
        }
    }
}
