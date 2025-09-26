/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.utils.HighlightValidator;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HighlightValidatorTest extends OpenSearchTestCase {

    private SearchResponse mockResponse;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockResponse = mock(SearchResponse.class);
    }

    public void testValidConfiguration() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertTrue(validated.isValid());
        assertNull(validated.getValidationError());
    }

    public void testAlreadyInvalidConfig() {
        HighlightConfig invalidConfig = HighlightConfig.invalid("Already invalid");

        HighlightConfig validated = HighlightValidator.validate(invalidConfig, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Already invalid", validated.getValidationError());
    }

    public void testMissingFieldName() {
        HighlightConfig config = HighlightConfig.builder().fieldName("").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("No semantic highlight field found", validated.getValidationError());
    }

    public void testMissingModelId() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Model ID is required for semantic highlighting", validated.getValidationError());
    }

    public void testMissingQueryText() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Query text is required for semantic highlighting", validated.getValidationError());
    }

    public void testNoSearchHits() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[0]);

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("No search hits to highlight", validated.getValidationError());
    }

    public void testNullResponse() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        HighlightConfig validated = HighlightValidator.validate(config, null);

        assertFalse(validated.isValid());
        assertEquals("No search hits to highlight", validated.getValidationError());
    }

    public void testInvalidBatchSize() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(true)
            .maxBatchSize(0)
            .build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Invalid max batch size: 0", validated.getValidationError());
    }

    public void testValidBatchConfiguration() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(true)
            .maxBatchSize(100)
            .build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = HighlightValidator.validate(config, mockResponse);

        assertTrue(validated.isValid());
        assertNull(validated.getValidationError());
    }
}
