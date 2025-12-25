/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollapseDataCollectorTests extends OpenSearchTestCase {

    public void testConstructor_whenBytesRefType_thenSuccessful() {
        CollapseTopFieldDocs testCollapseTopFieldDocs = new CollapseTopFieldDocs(
            null,
            null,
            null,
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { new BytesRef("test") }
        );
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(null, List.of(testCollapseTopFieldDocs), null, null);

        CollapseDTO mockCollapseDTO = mock(CollapseDTO.class);
        when(mockCollapseDTO.getCollapseQueryTopDocs()).thenReturn(List.of(compoundTopDocs));
        when(mockCollapseDTO.getIndexOfFirstNonEmpty()).thenReturn(0);

        CollapseDataCollector<?> collector = new CollapseDataCollector<>(mockCollapseDTO);
        assertNotNull(collector);
    }

    public void testConstructor_whenLongType_thenSuccessful() {
        CollapseTopFieldDocs testCollapseTopFieldDocs = new CollapseTopFieldDocs(
            null,
            null,
            null,
            new SortField[] { SortField.FIELD_SCORE },
            new Object[] { 1L }
        );
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(null, List.of(testCollapseTopFieldDocs), null, null);

        CollapseDTO mockCollapseDTO = mock(CollapseDTO.class);
        when(mockCollapseDTO.getCollapseQueryTopDocs()).thenReturn(List.of(compoundTopDocs));
        when(mockCollapseDTO.getIndexOfFirstNonEmpty()).thenReturn(0);

        CollapseDataCollector<?> collector = new CollapseDataCollector<>(mockCollapseDTO);
        assertNotNull(collector);
    }
}
