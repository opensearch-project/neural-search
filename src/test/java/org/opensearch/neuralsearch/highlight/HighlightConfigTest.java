/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HighlightConfigTest {

    @Test
    public void testBuilderWithAllFields() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model-id")
            .queryText("test query")
            .preTag("<strong>")
            .postTag("</strong>")
            .batchInference(true)
            .maxBatchSize(50)
            .build();

        assertEquals("content", config.getFieldName());
        assertEquals("test-model-id", config.getModelId());
        assertEquals("test query", config.getQueryText());
        assertEquals("<strong>", config.getPreTag());
        assertEquals("</strong>", config.getPostTag());
        assertTrue(config.isBatchInference());
        assertEquals(50, config.getMaxBatchSize());
        assertTrue(config.isValid());
        assertNull(config.getValidationError());
    }

    @Test
    public void testBuilderWithDefaults() {
        HighlightConfig config = HighlightConfig.builder().fieldName("title").modelId("model-123").queryText("search text").build();

        assertEquals("title", config.getFieldName());
        assertEquals("model-123", config.getModelId());
        assertEquals("search text", config.getQueryText());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PRE_TAG, config.getPreTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_POST_TAG, config.getPostTag());
        assertFalse(config.isBatchInference());
        assertEquals(SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE, config.getMaxBatchSize());
        assertTrue(config.isValid());
    }

    @Test
    public void testInvalidConfiguration() {
        HighlightConfig config = HighlightConfig.invalid("Test error message");

        assertFalse(config.isValid());
        assertEquals("Test error message", config.getValidationError());
        assertNotNull(config.getFieldName());
        assertNotNull(config.getModelId());
        assertNotNull(config.getQueryText());
    }

    @Test
    public void testWithValidationError() {
        HighlightConfig validConfig = HighlightConfig.builder().fieldName("content").modelId("model-id").queryText("query").build();

        assertTrue(validConfig.isValid());

        HighlightConfig invalidConfig = validConfig.withValidationError("Validation failed");

        assertFalse(invalidConfig.isValid());
        assertEquals("Validation failed", invalidConfig.getValidationError());
        // Other fields should remain the same
        assertEquals("content", invalidConfig.getFieldName());
        assertEquals("model-id", invalidConfig.getModelId());
        assertEquals("query", invalidConfig.getQueryText());
    }

    @Test
    public void testToBuilder() {
        HighlightConfig original = HighlightConfig.builder()
            .fieldName("content")
            .modelId("model-1")
            .queryText("original query")
            .batchInference(true)
            .maxBatchSize(200)
            .build();

        HighlightConfig modified = original.toBuilder().queryText("modified query").maxBatchSize(300).build();

        // Original should be unchanged
        assertEquals("original query", original.getQueryText());
        assertEquals(200, original.getMaxBatchSize());

        // Modified should have new values
        assertEquals("modified query", modified.getQueryText());
        assertEquals(300, modified.getMaxBatchSize());
        // Other fields should remain the same
        assertEquals("content", modified.getFieldName());
        assertEquals("model-1", modified.getModelId());
        assertTrue(modified.isBatchInference());
    }

    @Test(expected = NullPointerException.class)
    public void testRequiredFieldName() {
        HighlightConfig.builder().modelId("model-id").queryText("query").build();
    }

    @Test(expected = NullPointerException.class)
    public void testRequiredModelId() {
        HighlightConfig.builder().fieldName("field").queryText("query").build();
    }

    @Test(expected = NullPointerException.class)
    public void testRequiredQueryText() {
        HighlightConfig.builder().fieldName("field").modelId("model-id").build();
    }
}
