/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseFieldUtilsTests extends OpenSearchTestCase {

    private static final String TEST_INDEX_NAME = "test_index";
    private static final String TEST_SPARSE_FIELD_NAME = "test_sparse_field";
    private static final String TEST_PARENT_FIELD_NAME = "test_parent_field";

    @Mock
    private IndexMetadata indexMetadata;
    @Mock
    private ClusterService clusterService;
    @Mock
    private Metadata metadata;
    @Mock
    private ClusterState clusterState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
    }

    public void testGetSparseAnnFields_whenNullSparseIndex_thenReturnEmptySet() {
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(null, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNullIndexMetadata_thenReturnEmptySet() {
        configureSparseIndexSetting(true);
        when(metadata.index(anyString())).thenReturn(null);
        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNonSparseIndex_thenReturnEmptySet() {
        // Setup mock cluster service with non-sparse index
        configureSparseIndexSetting(false);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNullMappingMetaData_thenReturnEmptySet() {
        // Setup mock cluster service with null mapping metadata
        configureSparseIndexSetting(true);
        when(indexMetadata.mapping()).thenReturn(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNullSourceAsMap_thenReturnEmptySet() {
        // Setup mock cluster service with null mapping metadata
        configureSparseIndexSetting(true);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(mappingMetadata.sourceAsMap()).thenReturn(null);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenEmptyProperties_thenReturnEmptySet() {
        // Setup mock cluster service with empty properties
        configureIndexMappingProperties(Map.of());

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenNonSeismicField_thenReturnEmptySet() {
        // Setup mock cluster service with non-seismic field
        Map<String, Object> properties = TestsPrepareUtils.createFieldMappingProperties(
            false,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(0, SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService).size());
    }

    public void testGetSparseAnnFields_whenSeismicField_thenReturnField() {
        // Setup mock cluster service with seismic field
        Map<String, Object> properties = TestsPrepareUtils.createFieldMappingProperties(
            true,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(Set.of(TEST_SPARSE_FIELD_NAME), SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService));
    }

    public void testGetSparseAnnFields_whenNestedSeismicField_thenReturnField() {
        // Setup mock cluster service with nested seismic field
        Map<String, Object> properties = createNestedFieldMappingProperties(
            true,
            TEST_PARENT_FIELD_NAME,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        assertEquals(
            Set.of(TEST_PARENT_FIELD_NAME + "." + TEST_SPARSE_FIELD_NAME),
            SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService)
        );
    }

    private void configureSparseIndexSetting(boolean isSparseIndex) {
        Settings settings = Settings.builder().put("index.sparse", isSparseIndex).build();
        when(indexMetadata.getSettings()).thenReturn(settings);
    }

    private void configureIndexMappingProperties(Map<String, Object> properties) {
        MappingMetadata mappingMetadata = new MappingMetadata("_doc", properties);
        configureSparseIndexSetting(true);
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    }

    private Map<String, Object> createNestedFieldMappingProperties(boolean isSeismicField, String parentField, List<String> sparseFields) {
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> nestedFieldMapping = new HashMap<>();
        Map<String, Object> sparseFieldMapping = new HashMap<>();
        for (String sparseField : sparseFields) {
            Map<String, Object> sparseFieldProperties = new HashMap<>();
            sparseFieldProperties.put("type", isSeismicField ? "sparse_vector" : "rank_features");

            sparseFieldMapping.put(sparseField, sparseFieldProperties);
        }
        nestedFieldMapping.put("properties", sparseFieldMapping);
        properties.put("properties", Map.of(parentField, nestedFieldMapping));
        return properties;
    }
}
