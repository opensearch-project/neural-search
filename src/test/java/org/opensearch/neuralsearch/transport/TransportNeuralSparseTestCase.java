/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

public class TransportNeuralSparseTestCase extends OpenSearchTestCase {
    @Mock
    protected ClusterState clusterState;

    @Mock
    protected Metadata metadata;

    @Mock
    protected IndexMetadata indexMetadata;

    @Before
    public void setUpMocks() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Setup mocks for valid sparse indices
     */
    protected void setupValidSparseIndices() {
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
    }

    /**
     * Setup mocks for invalid sparse indices
     */
    protected void setupInvalidSparseIndices() {
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());
    }

    /**
     * Setup mocks for a mix of valid and invalid sparse indices
     * "valid-index" will be configured as a valid sparse index
     * All other indices will be configured as invalid
     */
    protected void setupMixedSparseIndices() {
        when(clusterState.metadata()).thenReturn(metadata);

        IndexMetadata validIndexMetadata = mock(IndexMetadata.class);
        IndexMetadata invalidIndexMetadata = mock(IndexMetadata.class);

        when(validIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
        when(invalidIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        when(metadata.index(anyString())).thenAnswer(invocation -> {
            String indexName = invocation.getArgument(0);
            if ("valid-index".equals(indexName)) {
                return validIndexMetadata;
            } else {
                return invalidIndexMetadata;
            }
        });
    }

    /**
     * Setup mocks for null index metadata (index doesn't exist)
     */
    protected void setupNullIndexMetadata() {
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(null);
    }

    /**
     * Setup mocks for null metadata
     */
    protected void setupNullMetadata() {
        when(clusterState.metadata()).thenReturn(null);
    }

    /**
     * Setup mocks for missing sparse index setting (defaults to false)
     */
    protected void setupMissingSparseIndexSetting() {
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().build()); // No sparse setting
    }

    /**
     * Setup mocks for null settings
     */
    protected void setupNullSettings() {
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(null);
    }
}
