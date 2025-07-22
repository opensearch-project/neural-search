/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.collector;

import org.apache.lucene.search.Collector;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MockFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactory;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactoryDTO;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_COLLAPSE_DOCS_PER_GROUP;

public class HybridCollectorFactoryTests extends OpenSearchTestCase {

    private final static int DOCS_PER_GROUP_PER_SUBQUERY = 10;

    public void testCreateCollector_whenKeywordCollapse_thenSuccessful() {
        KeywordFieldMapper.KeywordFieldType fieldType = mock(KeywordFieldMapper.KeywordFieldType.class);
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(HYBRID_COLLAPSE_DOCS_PER_GROUP.getKey(), DOCS_PER_GROUP_PER_SUBQUERY);

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settingsBuilder).build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);
        when(searchContext.collapse()).thenReturn(collapseContext);
        when(searchContext.indexShard()).thenReturn(indexShard);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);
        when(mockDTO.getNumHits()).thenReturn(5);

        Collector collector = HybridCollectorFactory.createCollector(mockDTO);
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    public void testCreateCollector_whenNumericCollapse_thenSuccessful() {
        NumberFieldMapper.NumberFieldType fieldType = mock(NumberFieldMapper.NumberFieldType.class);
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(HYBRID_COLLAPSE_DOCS_PER_GROUP.getKey(), DOCS_PER_GROUP_PER_SUBQUERY);

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settingsBuilder).build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);
        when(searchContext.collapse()).thenReturn(collapseContext);
        when(searchContext.indexShard()).thenReturn(indexShard);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);
        when(mockDTO.getNumHits()).thenReturn(5);

        Collector collector = HybridCollectorFactory.createCollector(mockDTO);
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    public void testCreateCollector_whenInvalidCollapseType_thenFail() {
        MockFieldMapper.FakeFieldType fieldType = new MockFieldMapper.FakeFieldType("test");
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(HYBRID_COLLAPSE_DOCS_PER_GROUP.getKey(), DOCS_PER_GROUP_PER_SUBQUERY);

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settingsBuilder).build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);
        when(searchContext.collapse()).thenReturn(collapseContext);
        when(searchContext.indexShard()).thenReturn(indexShard);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);

        expectThrows(IllegalStateException.class, () -> HybridCollectorFactory.createCollector(mockDTO));
    }
}
