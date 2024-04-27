/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridDisiWrapperTests extends OpenSearchQueryTestCase {

    public void testSubQueryIndex_whenCreateNewInstanceAndSetIndex_thenSuccessful() {
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator docIdSetIterator = mock(DocIdSetIterator.class);
        when(scorer.iterator()).thenReturn(docIdSetIterator);
        int subQueryIndex = 2;
        HybridDisiWrapper hybridDisiWrapper = new HybridDisiWrapper(scorer, subQueryIndex);
        assertEquals(2, hybridDisiWrapper.getSubQueryIndex());
    }
}
