/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.collector;

import org.apache.lucene.search.Collector;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MockFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactory;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactoryDTO;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridCollectorFactoryTests extends OpenSearchTestCase {

    public void testCreateCollector_whenKeywordCollapse_thenSuccessful() {
        KeywordFieldMapper.KeywordFieldType fieldType = mock(KeywordFieldMapper.KeywordFieldType.class);
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getCollapseContext()).thenReturn(collapseContext);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);

        Collector collector = HybridCollectorFactory.createCollector(mockDTO);
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    public void testCreateCollector_whenNumericCollapse_thenSuccessful() {
        NumberFieldMapper.NumberFieldType fieldType = mock(NumberFieldMapper.NumberFieldType.class);
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getCollapseContext()).thenReturn(collapseContext);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);

        Collector collector = HybridCollectorFactory.createCollector(mockDTO);
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    public void testCreateCollector_whenInvalidCollapseType_thenFail() {
        MockFieldMapper.FakeFieldType fieldType = new MockFieldMapper.FakeFieldType("test");
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldType()).thenReturn(fieldType);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.size()).thenReturn(1);

        HybridCollectorFactoryDTO mockDTO = mock(HybridCollectorFactoryDTO.class);
        when(mockDTO.getCollapseContext()).thenReturn(collapseContext);
        when(mockDTO.getSearchContext()).thenReturn(searchContext);

        expectThrows(IllegalStateException.class, () -> HybridCollectorFactory.createCollector(mockDTO));
    }
}
