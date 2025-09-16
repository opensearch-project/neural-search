/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HighlightValidatorTest {

    private HighlightValidator validator;
    private SearchResponse mockResponse;

    @Before
    public void setUp() {
        validator = new HighlightValidator();
        mockResponse = mock(SearchResponse.class);
    }

    @Test
    public void testValidConfiguration() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertTrue(validated.isValid());
        assertNull(validated.getValidationError());
    }

    @Test
    public void testAlreadyInvalidConfig() {
        HighlightConfig invalidConfig = HighlightConfig.invalid("Already invalid");

        HighlightConfig validated = validator.validate(invalidConfig, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Already invalid", validated.getValidationError());
    }

    @Test
    public void testMissingFieldName() {
        HighlightConfig config = HighlightConfig.builder().fieldName("").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("No semantic highlight field found", validated.getValidationError());
    }

    @Test
    public void testMissingModelId() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Model ID is required for semantic highlighting", validated.getValidationError());
    }

    @Test
    public void testMissingQueryText() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Query text is required for semantic highlighting", validated.getValidationError());
    }

    @Test
    public void testNoSearchHits() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[0]);

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("No search hits to highlight", validated.getValidationError());
    }

    @Test
    public void testNullResponse() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        HighlightConfig validated = validator.validate(config, null);

        assertFalse(validated.isValid());
        assertEquals("No search hits to highlight", validated.getValidationError());
    }

    @Test
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

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertEquals("Invalid max batch size: 0", validated.getValidationError());
    }

    @Test
    public void testExceededBatchSize() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(true)
            .maxBatchSize(SemanticHighlightingConstants.ABSOLUTE_MAX_BATCH_SIZE + 1)
            .build();

        SearchHits searchHits = mock(SearchHits.class);
        when(mockResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(new SearchHit[] { mock(SearchHit.class) });

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertFalse(validated.isValid());
        assertTrue(validated.getValidationError().startsWith("Max batch size exceeds limit:"));
    }

    @Test
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

        HighlightConfig validated = validator.validate(config, mockResponse);

        assertTrue(validated.isValid());
        assertNull(validated.getValidationError());
    }
}
