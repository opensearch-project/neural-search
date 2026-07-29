/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import org.junit.Before;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SemanticModelSelectionSettingsAccessorTests extends OpenSearchTestCase {

    private static final String SPARSE_ENGLISH_KEY = "plugins.neural_search.model_selection.model_id.sparse.english";

    private ClusterService clusterService;

    @Before
    public void setup() {
        clusterService = mock(ClusterService.class);
    }

    private void stub(final Settings nodeSettings, final Settings clusterMetadataSettings) {
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.settings()).thenReturn(clusterMetadataSettings);
    }

    public void testGetModelId_whenConfiguredInClusterSettings_thenReturnIt() {
        stub(Settings.EMPTY, Settings.builder().put(SPARSE_ENGLISH_KEY, "model_1").build());
        final SemanticModelSelectionSettingsAccessor accessor = new SemanticModelSelectionSettingsAccessor(clusterService);
        assertEquals("model_1", accessor.getModelId("sparse.english"));
    }

    public void testGetModelId_whenConfiguredInNodeSettings_thenReturnIt() {
        stub(Settings.builder().put(SPARSE_ENGLISH_KEY, "node_model").build(), Settings.EMPTY);
        final SemanticModelSelectionSettingsAccessor accessor = new SemanticModelSelectionSettingsAccessor(clusterService);
        assertEquals("node_model", accessor.getModelId("sparse.english"));
    }

    public void testGetModelId_whenNotConfigured_thenReturnNull() {
        stub(Settings.EMPTY, Settings.EMPTY);
        final SemanticModelSelectionSettingsAccessor accessor = new SemanticModelSelectionSettingsAccessor(clusterService);
        assertNull(accessor.getModelId("sparse.english"));
    }

    public void testGetModelId_whenEmptyValue_thenReturnNull() {
        stub(Settings.EMPTY, Settings.builder().put(SPARSE_ENGLISH_KEY, "").build());
        final SemanticModelSelectionSettingsAccessor accessor = new SemanticModelSelectionSettingsAccessor(clusterService);
        assertNull(accessor.getModelId("sparse.english"));
    }
}
