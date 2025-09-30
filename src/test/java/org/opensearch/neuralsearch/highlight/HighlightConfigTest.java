/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.test.OpenSearchTestCase;

public class HighlightConfigTest extends OpenSearchTestCase {

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

    public void testInvalidConfiguration() {
        HighlightConfig config = HighlightConfig.invalid("Test error message");

        assertFalse(config.isValid());
        assertEquals("Test error message", config.getValidationError());
        assertNotNull(config.getFieldName());
        assertNotNull(config.getModelId());
        assertNotNull(config.getQueryText());
    }

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

    public void testRequiredFieldName() {
        expectThrows(NullPointerException.class, () -> { HighlightConfig.builder().modelId("model-id").queryText("query").build(); });
    }

    public void testRequiredModelId() {
        expectThrows(NullPointerException.class, () -> { HighlightConfig.builder().fieldName("field").queryText("query").build(); });
    }

    public void testRequiredQueryText() {
        expectThrows(NullPointerException.class, () -> { HighlightConfig.builder().fieldName("field").modelId("model-id").build(); });
    }

    public void testWithModelType() {
        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        assertNull(config.getModelType());

        HighlightConfig configWithType = config.withModelType(FunctionName.REMOTE);
        assertEquals(FunctionName.REMOTE, configWithType.getModelType());
        // Other fields should remain the same
        assertEquals("content", configWithType.getFieldName());
        assertEquals("test-model", configWithType.getModelId());
    }

    public void testModelSupportsBatchInference() {
        // Test with REMOTE model
        HighlightConfig remoteConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("remote-model")
            .queryText("query")
            .build()
            .withModelType(FunctionName.REMOTE);

        assertTrue(remoteConfig.modelSupportsBatchInference());

        // Test with QUESTION_ANSWERING model
        HighlightConfig qaConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("qa-model")
            .queryText("query")
            .build()
            .withModelType(FunctionName.QUESTION_ANSWERING);

        assertFalse(qaConfig.modelSupportsBatchInference());

        // Test with TEXT_EMBEDDING model
        HighlightConfig embeddingConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("embed-model")
            .queryText("query")
            .build()
            .withModelType(FunctionName.TEXT_EMBEDDING);

        assertFalse(embeddingConfig.modelSupportsBatchInference());
    }

    public void testValidateBatchInferenceWithRemoteModel() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("remote-model")
            .queryText("query")
            .batchInference(true)
            .build()
            .withModelType(FunctionName.REMOTE);

        String validationError = config.validateBatchInference();
        assertNull(validationError);
    }

    public void testValidateBatchInferenceWithLocalModel() {
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("local-model")
            .queryText("query")
            .batchInference(true)
            .build()
            .withModelType(FunctionName.QUESTION_ANSWERING);

        String validationError = config.validateBatchInference();
        assertNotNull(validationError);
        assertTrue(validationError.contains("does not support batch inference"));
        assertTrue(validationError.contains("QUESTION_ANSWERING"));
        assertTrue(validationError.contains("local-model"));
    }

    public void testValidateBatchInferenceWithBatchDisabled() {
        // Batch inference disabled should work with any model type
        HighlightConfig qaConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("qa-model")
            .queryText("query")
            .batchInference(false)
            .build()
            .withModelType(FunctionName.QUESTION_ANSWERING);

        assertNull(qaConfig.validateBatchInference());

        HighlightConfig remoteConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("remote-model")
            .queryText("query")
            .batchInference(false)
            .build()
            .withModelType(FunctionName.REMOTE);

        assertNull(remoteConfig.validateBatchInference());
    }

    public void testValidateBatchInferenceWithNullModelType() {
        // If model type is not set yet, validation should pass
        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("model")
            .queryText("query")
            .batchInference(true)
            .build();

        assertNull(config.getModelType());
        assertNull(config.validateBatchInference());
    }
}
