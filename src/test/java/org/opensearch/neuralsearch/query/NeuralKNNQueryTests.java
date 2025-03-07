/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.Query;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeuralKNNQueryTests extends OpenSearchTestCase {

    public void testNeuralKNNQuery() throws IOException {
        Query mockKnnQuery = mock(Query.class);
        String originalQueryText = "test query";
        NeuralKNNQuery query = new NeuralKNNQuery(mockKnnQuery, originalQueryText);

        // Test toString
        when(mockKnnQuery.toString("field")).thenReturn("test_query");
        assertEquals("toString should delegate to underlying query", "test_query", query.toString("field"));

        // Test createWeight
        when(mockKnnQuery.createWeight(any(), any(), anyFloat())).thenReturn(null);
        query.createWeight(null, null, 1.0f);
        verify(mockKnnQuery).createWeight(any(), any(), anyFloat());

        // Test equals and hashCode
        NeuralKNNQuery query2 = new NeuralKNNQuery(mockKnnQuery, originalQueryText);
        assertEquals("Same underlying query should be equal", query, query2);
        assertEquals("Same underlying query should have same hash code", query.hashCode(), query2.hashCode());

        // Test originalQueryText getter
        assertEquals("Original query text should match", originalQueryText, query.getOriginalQueryText());

        // Test not equals with different originalQueryText
        NeuralKNNQuery query3 = new NeuralKNNQuery(mockKnnQuery, "different query");
        assertNotEquals("Different original query text should not be equal", query, query3);
        assertNotEquals("Different original query text should have different hash code", query.hashCode(), query3.hashCode());
    }
}
