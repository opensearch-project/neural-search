/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessorUtilsMaxTokenCountTests extends OpenSearchTestCase {

    private static final String CONCRETE_INDEX = "my_index";
    private static final String ALIAS_NAME = "my_alias";
    private static final int CLUSTER_DEFAULT = 10000;
    private static final int INDEX_SETTING = 15000;

    public void testGetMaxTokenCount_whenConcreteIndex_thenReturnsIndexSetting() {
        Settings clusterSettings = Settings.builder().put("index.analyze.max_token_count", CLUSTER_DEFAULT).build();
        ClusterService clusterService = mockClusterService(CONCRETE_INDEX, INDEX_SETTING, null);
        Map<String, Object> sourceAndMetadata = createSourceAndMetadata(CONCRETE_INDEX);

        int result = ProcessorUtils.getMaxTokenCount(sourceAndMetadata, clusterSettings, clusterService);

        assertEquals(INDEX_SETTING, result);
    }

    public void testGetMaxTokenCount_whenAlias_thenResolvesAndReturnsIndexSetting() {
        Settings clusterSettings = Settings.builder().put("index.analyze.max_token_count", CLUSTER_DEFAULT).build();
        ClusterService clusterService = mockClusterService(null, INDEX_SETTING, ALIAS_NAME);
        Map<String, Object> sourceAndMetadata = createSourceAndMetadata(ALIAS_NAME);

        int result = ProcessorUtils.getMaxTokenCount(sourceAndMetadata, clusterSettings, clusterService);

        assertEquals(INDEX_SETTING, result);
    }

    public void testGetMaxTokenCount_whenIndexNotFound_thenReturnsClusterDefault() {
        Settings clusterSettings = Settings.builder().put("index.analyze.max_token_count", CLUSTER_DEFAULT).build();
        ClusterService clusterService = mockClusterService(null, 0, null);
        Map<String, Object> sourceAndMetadata = createSourceAndMetadata("nonexistent");

        int result = ProcessorUtils.getMaxTokenCount(sourceAndMetadata, clusterSettings, clusterService);

        assertEquals(CLUSTER_DEFAULT, result);
    }

    private Map<String, Object> createSourceAndMetadata(String indexName) {
        Map<String, Object> map = new HashMap<>();
        map.put(IndexFieldMapper.NAME, indexName);
        return map;
    }

    private ClusterService mockClusterService(String concreteIndexName, int maxTokenCount, String aliasName) {
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        // Set up concrete index lookup
        if (concreteIndexName != null) {
            IndexMetadata indexMetadata = createIndexMetadata(maxTokenCount);
            when(metadata.index(concreteIndexName)).thenReturn(indexMetadata);
        } else {
            when(metadata.index(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
        }

        // Set up alias resolution via IndicesLookup
        SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
        if (aliasName != null) {
            IndexMetadata indexMetadata = createIndexMetadata(maxTokenCount);
            IndexAbstraction indexAbstraction = mock(IndexAbstraction.class);
            when(indexAbstraction.getWriteIndex()).thenReturn(indexMetadata);
            indicesLookup.put(aliasName, indexAbstraction);
        }
        when(metadata.getIndicesLookup()).thenReturn(indicesLookup);

        return clusterService;
    }

    private IndexMetadata createIndexMetadata(int maxTokenCount) {
        Settings indexSettings = Settings.builder()
            .put("index.analyze.max_token_count", maxTokenCount)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.opensearch.Version.CURRENT.id)
            .build();
        return IndexMetadata.builder(CONCRETE_INDEX).settings(indexSettings).build();
    }
}
