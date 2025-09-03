/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

public class RestUtilsTests extends RestNeuralSparseTestCase {

    private static final String API_OPERATION = "test_operation";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testValidateSparseIndicesWithValidSparseIndices() {
        // Setup
        Index[] indices = { new Index("valid-index-1", "uuid1"), new Index("valid-index-2", "uuid2") };

        setupValidSparseIndices();

        // Execute - should not throw exception
        RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION);
    }

    public void testValidateSparseIndicesWithInvalidSparseIndices() {
        // Setup
        Index[] indices = { new Index("invalid-index-1", "uuid1"), new Index("invalid-index-2", "uuid2") };

        setupInvalidSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[invalid-index-1, invalid-index-2]"));
        assertTrue(exception.getMessage().contains("test_operation"));
        assertTrue(exception.getMessage().contains("Request rejected"));
    }

    public void testValidateSparseIndicesWithMixedValidInvalidIndices() {
        // Setup
        Index[] indices = { new Index("valid-index", "uuid1"), new Index("invalid-index", "uuid2") };

        setupMixedSparseIndices();

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[invalid-index]"));
        assertFalse(exception.getMessage().contains("[valid-index]"));
    }

    public void testValidateSparseIndicesWithEmptyIndicesArray() {
        // Setup
        Index[] indices = {};

        // Execute - should not throw exception
        RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION);
    }

    public void testValidateSparseIndicesWithNullClusterService() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, null, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithNullClusterState() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(null);

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithNullMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(null);

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithNullIndexMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(null);

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithNullSettings() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(null);

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithFalseSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }

    public void testValidateSparseIndicesWithMissingSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().build()); // No sparse setting

        // Execute & Verify
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, API_OPERATION)
        );

        assertTrue(exception.getMessage().contains("[test-index]"));
    }
}
