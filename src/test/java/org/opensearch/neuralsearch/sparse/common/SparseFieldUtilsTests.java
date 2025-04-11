/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseFieldUtilsTests extends OpenSearchTestCase {

    private static final String TEST_INDEX_NAME = "test_index";
    private static final String TEST_SPARSE_FIELD_NAME = "test_sparse_field";

    private IndexMetadata indexMetadata;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
    }

    public void testGetSparseAnnFields_whenNullSparseIndex_thenReturnEmptySet() {
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(null).size());
    }

    public void testGetSparseAnnFields_whenNonSparseIndex_thenReturnEmptySet() {
        // Setup mock cluster service with non-sparse index
        configureSparseIndexSetting(false);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
    }

    public void testGetSparseAnnFields_whenNullMappingMetaData_thenReturnEmptySet() {
        // Setup mock cluster service with null mapping metadata
        configureIndexMapping(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
    }

    public void testGetSparseAnnFields_whenEmptyProperties_thenReturnEmptySet() {
        // Setup mock cluster service with empty properties
        configureIndexMappingProperties(Map.of());

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
    }

    public void testGetSparseAnnFields_whenNonSeismicField_thenReturnEmptySet() {
        // Setup mock cluster service with non-seismic field
        Map<String, Object> properties = createFieldMappingProperties(false);
        configureIndexMappingProperties(properties);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME).size());
    }

    public void testGetSparseAnnFields_whenSeismicField_thenReturnField() {
        // Setup mock cluster service with seismic field
        Map<String, Object> properties = createFieldMappingProperties(true);
        configureIndexMappingProperties(properties);

        assertEquals(Set.of(TEST_SPARSE_FIELD_NAME), SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME));
    }

    private void initializeMockClusterService() {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);

        indexMetadata = mock(IndexMetadata.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
    }

    private void configureSparseIndexSetting(boolean isSparseIndex) {
        initializeMockClusterService();
        Settings settings = Settings.builder().put("index.sparse", isSparseIndex).build();
        when(indexMetadata.getSettings()).thenReturn(settings);
    }

    private void configureIndexMapping(MappingMetadata mappingMetadata) {
        configureSparseIndexSetting(true);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    }

    private void configureIndexMappingProperties(Map<String, Object> properties) {
        MappingMetadata mappingMetadata = new MappingMetadata("_doc", properties);
        configureIndexMapping(mappingMetadata);
    }

    private Map<String, Object> createFieldMappingProperties(boolean isSeismicField) {
        Map<String, Object> sparseFieldMapping = new HashMap<>();
        Map<String, Object> sparseFieldProperties = new HashMap<>();
        sparseFieldProperties.put("type", isSeismicField ? "sparse_tokens" : "rank_features");
        sparseFieldMapping.put(TEST_SPARSE_FIELD_NAME, sparseFieldProperties);

        Map<String, Object> properties = new HashMap<>();
        properties.put("properties", sparseFieldMapping);
        return properties;
    }
}
