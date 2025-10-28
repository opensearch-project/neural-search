/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

public class TransportUtilsTests extends TransportNeuralSparseTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testValidateSparseIndicesWithValidSparseIndices() {
        // Setup
        String[] concreteIndices = { "valid-index-1", "valid-index-2" };
        setupValidSparseIndices();

        // Execute - should not throw exception
        TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action");
    }

    public void testValidateSparseIndicesWithSingleValidSparseIndex() {
        // Setup
        String[] concreteIndices = { "valid-index" };
        setupValidSparseIndices();

        // Execute - should not throw exception
        TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action");
    }

    public void testValidateSparseIndicesWithInvalidSparseIndices() {
        // Setup
        String[] concreteIndices = { "invalid-index-1", "invalid-index-2" };
        setupInvalidSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[invalid-index-1, invalid-index-2]"));
        assertTrue(exception.getMessage().contains("neural_sparse_warmup_action"));
        assertTrue(exception.getMessage().contains("Request rejected"));
        assertTrue(exception.getMessage().contains("don't support"));
    }

    public void testValidateSparseIndicesWithSingleInvalidSparseIndex() {
        // Setup
        String[] concreteIndices = { "invalid-index" };
        setupInvalidSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[invalid-index]"));
        assertTrue(exception.getMessage().contains("neural_sparse_warmup_action"));
    }

    public void testValidateSparseIndicesWithMixedValidInvalidIndices() {
        // Setup
        String[] concreteIndices = { "valid-index", "invalid-index" };
        setupMixedSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertEquals(
            "Request rejected. Indices [invalid-index] don't support neural_sparse_warmup_action operation.",
            exception.getMessage()
        );
    }

    public void testValidateSparseIndicesWithMultipleMixedIndices() {
        // Setup
        String[] concreteIndices = { "valid-index", "invalid-index-1", "invalid-index-2" };
        setupMixedSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertEquals(
            "Request rejected. Indices [invalid-index-1, invalid-index-2] don't support neural_sparse_warmup_action operation.",
            exception.getMessage()
        );
    }

    public void testValidateSparseIndicesWithEmptyIndicesArray() {
        // Setup
        String[] concreteIndices = {};

        // Execute - should not throw exception
        TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action");
    }

    public void testValidateSparseIndicesWithNullIndexMetadata() {
        // Setup
        String[] concreteIndices = { "non-existent-index" };
        setupNullIndexMetadata();

        // Execute & Verify
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertTrue(exception.getMessage().contains("Index metadata not found for concrete index: non-existent-index"));
    }

    public void testValidateSparseIndicesWithMultipleNullIndexMetadata() {
        // Setup
        String[] concreteIndices = { "non-existent-index-1", "non-existent-index-2" };
        setupNullIndexMetadata();

        // Execute & Verify - should fail on the first index
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertTrue(exception.getMessage().contains("Index metadata not found for concrete index: non-existent-index-1"));
    }

    public void testValidateSparseIndicesWithNullMetadata() {
        // Setup
        String[] concreteIndices = { "test-index" };
        setupNullMetadata();

        // Execute & Verify
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );
    }

    public void testValidateSparseIndicesWithFalseSparseIndexSetting() {
        // Setup
        String[] concreteIndices = { "test-index" };
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithMissingSparseIndexSetting() {
        // Setup
        String[] concreteIndices = { "test-index" };
        setupMissingSparseIndexSetting();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithNullSettings() {
        // Setup
        String[] concreteIndices = { "test-index" };
        setupNullSettings();

        // Execute & Verify - SparseSettings.IS_SPARSE_INDEX_SETTING.get() should handle null gracefully
        // and return the default value (false)
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action")
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithTrueSparseIndexSetting() {
        // Setup
        String[] concreteIndices = { "test-index" };
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());

        // Execute - should not throw exception
        TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_warmup_action");
    }

    public void testValidateSparseIndicesWithCustomOperationName() {
        // Setup
        String[] concreteIndices = { "invalid-index" };
        String customOperation = "neural_sparse_clear_cache_action";
        setupInvalidSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportUtils.validateSparseIndices(clusterState, concreteIndices, customOperation)
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertTrue(exception.getMessage().contains("[invalid-index]"));
        assertTrue(exception.getMessage().contains("neural_sparse_clear_cache_action"));
        assertFalse(exception.getMessage().contains("neural_sparse_warmup_action"));
    }
}
