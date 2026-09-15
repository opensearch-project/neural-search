/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.After;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage of the native engine kill switch: with the gate closed a native index cannot
 * be created, cannot be written to, and cannot be queried.
 *
 * <p>Only the dynamic gate is exercised here. The static
 * {@link SparseSettings#SPARSE_NATIVE_ENGINE_FEATURE_ENABLED} flag comes from opensearch.yml and
 * cannot be flipped on a running cluster, so the AND of the two is covered in
 * {@link SparseSettingsTests}.
 */
public class SparseNativeEngineFeatureFlagIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-native-engine-flag";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    @ParametersFactory(argumentFormatting = "engine=%s")
    public static Collection<Object[]> parameters() {
        return List.<Object[]>of(new Object[] { SparseEngine.NATIVE });
    }

    public SparseNativeEngineFeatureFlagIT(SparseEngine engine) {
        super(engine);
    }

    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        // Leave the gate open so a suite running after this one is unaffected
        updateClusterSettings(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, true);
        super.tearDown();
    }

    private void disableNativeEngine() {
        updateClusterSettings(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, false);
    }

    private void createNativeIndex() throws Exception {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, 4);
    }

    private NeuralSparseQueryBuilder query() {
        return getNeuralSparseQueryBuilder(TEST_SPARSE_FIELD_NAME, 2, 1.0f, 10, Map.of("1000", 0.1f, "2000", 0.2f));
    }

    /** Sends a bulk request without asserting success, so the per-item error can be inspected. */
    @SneakyThrows
    private String rawBulk(int startingId, int docCount) {
        String payload = prepareSparseBulkIngestPayload(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            prepareIngestDocuments(docCount),
            null,
            startingId
        );
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
        Response response = makeRequest(
            client(),
            "POST",
            "_bulk",
            params,
            toHttpEntity(payload),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        return EntityUtils.toString(response.getEntity());
    }

    @SneakyThrows
    public void testCreateNativeIndexIsRejectedWhenDisabled() {
        disableNativeEngine();

        ResponseException exception = expectThrows(ResponseException.class, this::createNativeIndex);
        assertTrue(exception.getMessage(), exception.getMessage().contains(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED));
    }

    /**
     * An index created while the gate was open must stop accepting documents once it closes. The
     * rejection is per document, so the bulk response is a 200 carrying an item level error.
     */
    @SneakyThrows
    public void testIngestIntoExistingNativeIndexIsRejectedWhenDisabled() {
        createNativeIndex();
        assertFalse(rawBulk(1, 4).contains("\"error\""));

        disableNativeEngine();

        String body = rawBulk(100, 2);
        assertTrue(body, body.contains("\"error\""));
        assertTrue(body, body.contains(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED));

        deleteIndex(TEST_INDEX_NAME);
    }

    /**
     * Querying an existing native index must fail rather than silently fall back: the field's
     * mapping still says native, and the Lucene seismic path has no clustered postings to read.
     */
    @SneakyThrows
    public void testQueryExistingNativeIndexIsRejectedWhenDisabled() {
        createNativeIndex();
        rawBulk(1, 4);
        forceMerge(TEST_INDEX_NAME);
        // Proves the index is usable while the gate is open
        assertNotNull(search(TEST_INDEX_NAME, query(), 10));

        disableNativeEngine();

        ResponseException exception = expectThrows(ResponseException.class, () -> search(TEST_INDEX_NAME, query(), 10));
        assertTrue(exception.getMessage(), exception.getMessage().contains(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED));

        deleteIndex(TEST_INDEX_NAME);
    }

    /** Re-opening the gate restores the index that was refused while it was closed. */
    @SneakyThrows
    public void testReEnablingRestoresTheNativeIndex() {
        createNativeIndex();
        rawBulk(1, 4);
        forceMerge(TEST_INDEX_NAME);

        disableNativeEngine();
        expectThrows(ResponseException.class, () -> search(TEST_INDEX_NAME, query(), 10));

        updateClusterSettings(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, true);
        assertNotNull(search(TEST_INDEX_NAME, query(), 10));

        deleteIndex(TEST_INDEX_NAME);
    }
}
