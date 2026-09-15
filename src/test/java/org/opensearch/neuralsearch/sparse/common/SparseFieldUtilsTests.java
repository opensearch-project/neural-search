/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTERING_BATCH_SIZE;

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

    public void testGetSparseAnnFields_whenNestedSeismicField_andExceedMapDepth_thenThrowException() {
        // Setup mock cluster service with deeply nested seismic field that exceeds maxDepth
        Map<String, Object> properties = createNestedFieldMappingProperties(
            true,
            TEST_PARENT_FIELD_NAME,
            Collections.singletonList(TEST_SPARSE_FIELD_NAME)
        );
        configureIndexMappingProperties(properties);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            SparseFieldUtils.getSparseAnnFields(TEST_INDEX_NAME, clusterService, 1);
        });

        assertTrue(exception.getMessage().contains("exceeds maximum mapping depth limit"));
    }

    public void testGetMaxDepth_whenNullIndex_thenReturnDefaultDepth() {
        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(null, clusterService));
    }

    public void testGetMaxDepth_whenNullClusterService_thenReturnDefaultDepth() {
        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, null));
    }

    public void testGetMaxDepth_whenIndexNotFound_thenReturnDefaultDepth() {
        when(metadata.index(TEST_INDEX_NAME)).thenReturn(null);

        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    public void testGetMaxDepth_whenCustomDepthConfigured_thenReturnCustomDepth() {
        long customDepth = 50L;
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), customDepth).build();

        when(indexMetadata.getSettings()).thenReturn(settings);

        assertEquals(customDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    public void testGetMaxDepth_whenNoDepthConfigured_thenReturnDefaultDepth() {
        Settings settings = Settings.builder().build();
        when(indexMetadata.getSettings()).thenReturn(settings);

        long defaultDepth = MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(Settings.EMPTY);

        assertEquals(defaultDepth, SparseFieldUtils.getMaxDepth(TEST_INDEX_NAME, clusterService));
    }

    public void testGetSparseForwardIndex_withPerBlockAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(FORWARD_INDEX_FIELD)).thenReturn(SparseForwardIndex.PER_BLOCK.getName());

        assertEquals(SparseForwardIndex.PER_BLOCK, SparseFieldUtils.getSparseForwardIndex(fieldInfo));
    }

    public void testGetSparseForwardIndex_withMissingOrUnknownAttribute_fallsBackToDefault() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        assertEquals(SparseForwardIndex.DEFAULT, SparseFieldUtils.getSparseForwardIndex(fieldInfo));

        when(fieldInfo.getAttribute(FORWARD_INDEX_FIELD)).thenReturn("not_a_forward_index");
        assertEquals(SparseForwardIndex.DEFAULT, SparseFieldUtils.getSparseForwardIndex(fieldInfo));
    }

    public void testGetSparseForwardIndex_withNullFieldInfo_fallsBackToDefault() {
        assertEquals(SparseForwardIndex.DEFAULT, SparseFieldUtils.getSparseForwardIndex(null));
    }

    public void testGetClusteringBatchSize_withAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(CLUSTERING_BATCH_SIZE_FIELD)).thenReturn("64");

        assertEquals(64, SparseFieldUtils.getClusteringBatchSize(fieldInfo));
    }

    public void testGetClusteringBatchSize_withMissingOrUnparseableAttribute_fallsBackToDefault() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        assertEquals(DEFAULT_CLUSTERING_BATCH_SIZE, SparseFieldUtils.getClusteringBatchSize(fieldInfo));

        when(fieldInfo.getAttribute(CLUSTERING_BATCH_SIZE_FIELD)).thenReturn("not_an_integer");
        assertEquals(DEFAULT_CLUSTERING_BATCH_SIZE, SparseFieldUtils.getClusteringBatchSize(fieldInfo));
    }

    public void testGetClusteringBatchSize_withNullFieldInfo_fallsBackToDefault() {
        assertEquals(DEFAULT_CLUSTERING_BATCH_SIZE, SparseFieldUtils.getClusteringBatchSize(null));
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

    public void testGetSparseEngineDefaultsWhenFieldInfoIsMissing() {
        // A segment holding no document with the field has no FieldInfo for it
        assertEquals(SparseEngine.DEFAULT, SparseFieldUtils.getSparseEngine(null));
    }

    public void testGetSparseEngineDefaultsWhenAttributeIsAbsent() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(null);

        assertEquals(SparseEngine.DEFAULT, SparseFieldUtils.getSparseEngine(fieldInfo));
    }

    public void testGetSparseEngineReadsTheAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(SparseEngine.NATIVE.getName());

        assertEquals(SparseEngine.NATIVE, SparseFieldUtils.getSparseEngine(fieldInfo));
    }

    public void testFloatAttributeGetters() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(QUANTIZATION_CEILING_INGEST_FIELD, "3.5");
        attributes.put(CLUSTER_RATIO_FIELD, "0.1");
        attributes.put(SUMMARY_PRUNE_RATIO_FIELD, "0.4");
        when(fieldInfo.attributes()).thenReturn(attributes);

        assertEquals(3.5f, SparseFieldUtils.getQuantizationCeilingIngest(fieldInfo), 0.0f);
        assertEquals(0.1f, SparseFieldUtils.getClusterRatio(fieldInfo), 0.0f);
        assertEquals(0.4f, SparseFieldUtils.getSummaryPruneRatio(fieldInfo), 0.0f);
    }
}
