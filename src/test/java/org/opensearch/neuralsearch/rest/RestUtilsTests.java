/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;

import java.util.List;

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
        RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION);
    }

    public void testValidateSparseIndicesWithInvalidSparseIndices() {
        // Setup
        Index[] indices = { new Index("invalid-index-1", "uuid1"), new Index("invalid-index-2", "uuid2") };

        setupInvalidSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        List<String> invalidIndices = exception.getInvalidIndices();
        assertEquals(2, invalidIndices.size());
        assertTrue(invalidIndices.contains("invalid-index-1"));
        assertTrue(invalidIndices.contains("invalid-index-2"));
        assertTrue(exception.getMessage().contains("test_operation"));
        assertTrue(exception.getMessage().contains("Request rejected"));
    }

    public void testValidateSparseIndicesWithMixedValidInvalidIndices() {
        // Setup
        Index[] indices = { new Index("valid-index", "uuid1"), new Index("invalid-index", "uuid2") };

        setupMixedSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        List<String> invalidIndices = exception.getInvalidIndices();
        assertEquals(1, invalidIndices.size());
        assertTrue(invalidIndices.contains("invalid-index"));
        assertFalse(invalidIndices.contains("valid-index"));
    }

    public void testValidateSparseIndicesWithEmptyIndicesArray() {
        // Setup
        Index[] indices = {};

        // Execute - should not throw exception
        RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION);
    }

    public void testValidateSparseIndicesWithNullClusterService() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, null, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithNullClusterState() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithNullMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithNullIndexMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithNullSettings() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithFalseSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateSparseIndicesWithMissingSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().build()); // No sparse setting

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateSparseIndices(indices, clusterService, SPARSE_INDEX, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }
}
