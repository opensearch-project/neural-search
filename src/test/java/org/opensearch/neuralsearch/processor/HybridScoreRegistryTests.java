/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class HybridScoreRegistryTests extends OpenSearchTestCase {

    public void testStoreAndGet() {
        var mockSearchContext = mock(SearchPhaseContext.class);
        int docId = 1;
        float[] scores = new float[] { 0.4f, 0.6f };
        Map<Integer, float[]> expectedScores = Map.of(docId, scores);
        HybridScoreRegistry.store(mockSearchContext, Map.of(docId, scores));
        Map<Integer, float[]> actualScores = HybridScoreRegistry.get(mockSearchContext);
        assertEquals(expectedScores, actualScores);
    }

    public void testRemove() {
        var mockSearchContext = mock(SearchPhaseContext.class);
        HybridScoreRegistry.store(mockSearchContext, Map.of(1, new float[] { 0.4f, 0.6f }));
        HybridScoreRegistry.remove(mockSearchContext);
        assertNull(HybridScoreRegistry.get(mockSearchContext));
    }

}
