/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

public class RestNeuralSparseTestCase extends OpenSearchTestCase {
    @Mock
    protected ClusterService clusterService;

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
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
    }

    /**
     * Setup mocks for invalid sparse indices
     */
    protected void setupInvalidSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());
    }

    /**
     * Setup mocks for a mix of valid and invalid sparse indices
     * "valid-index" will be configured as a valid sparse index
     * All other indices will be configured as invalid
     */
    protected void setupMixedSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        IndexMetadata validIndexMetadata = mock(IndexMetadata.class);
        IndexMetadata invalidIndexMetadata = mock(IndexMetadata.class);

        when(validIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
        when(invalidIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        when(metadata.getIndexSafe(any(Index.class))).thenAnswer(invocation -> {
            Index index = invocation.getArgument(0);
            if ("valid-index".equals(index.getName())) {
                return validIndexMetadata;
            } else {
                return invalidIndexMetadata;
            }
        });
    }
}
