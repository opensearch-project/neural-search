/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.semantic;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class SemanticFieldProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING = "semantic_field_sparse_model_with_chunking_index";
    private static final String INDEX_WITH_SPARSE_MODEL = "semantic_field_sparse_model_index";
    protected static final String LEVEL_1_FIELD = "products";
    protected static final String LEVEL_2_FIELD = "product_description_semantic_info";
    protected static final String CHUNKS_FIELD = "chunks";
    protected static final String TEXT_FIELD = "text";
    protected static final String EMBEDDING_FIELD = "embedding";
    protected static final String MODEL_FIELD = "model";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/semantic/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/semantic/ingest_doc2.json").toURI()));
    private final String BULK_ITEM_TEMPLATE = Files.readString(
        Path.of(classLoader.getResource("processor/bulk_item_template.json").toURI())
    );

    public SemanticFieldProcessorIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testSemanticFieldProcessor_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL, "semantic/SemanticIndexMappings.json", modelId);

        // Test single document ingestion
        ingestDocument(INDEX_WITH_SPARSE_MODEL, INGEST_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_WITH_SPARSE_MODEL));

        Map<String, Object> source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "1").get("_source");
        assertSparseEmbeddings(source);
    }

    public void testSemanticFieldProcessor_withChunking_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, "semantic/SemanticIndexMappingsWithChunking.json", modelId);

        // Test document with chunking enabled
        ingestDocument(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, INGEST_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING));

        Map<String, Object> source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, "1").get("_source");
        assertEmbeddingWithChunks(source);
    }

    public void testSemanticFieldProcessor_batch_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL, "semantic/SemanticIndexMappings.json", modelId);

        // Test batch document ingestion
        ingestBatchDocumentWithBulk(
            INDEX_WITH_SPARSE_MODEL,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_WITH_SPARSE_MODEL));
        Map<String, Object> doc1Source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "batch_1").get("_source");
        Map<String, Object> doc2Source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "batch_2").get("_source");

        assertSparseEmbeddings(doc1Source);
        assertSparseEmbeddings(doc2Source);
    }

    public void testSemanticFieldProcessorWithChunking_batch_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, "semantic/SemanticIndexMappingsWithChunking.json", modelId);

        // Test batch document ingestion with chunking enabled
        ingestBatchDocumentWithBulk(
            INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING));

        Map<String, Object> doc1Source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, "batch_1").get("_source");
        Map<String, Object> doc2Source = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING, "batch_2").get("_source");

        assertEmbeddingWithChunks(doc1Source);
        assertEmbeddingWithChunks(doc2Source);
    }

    public void testUpdateScenarios_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL, "semantic/SemanticIndexMappings.json", modelId);

        // Initial ingestion
        ingestDocument(INDEX_WITH_SPARSE_MODEL, INGEST_DOC1, "1");
        Map<String, Object> originalDoc = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "1").get("_source");

        // Update with new model
        String newModelId = prepareSparseEncodingModel();
        loadModel(newModelId);
        updateIndexWithModelId(INDEX_WITH_SPARSE_MODEL, "semantic/UpdateMappingTemplate.json", newModelId);

        // Re-ingest and verify changes
        ingestDocument(INDEX_WITH_SPARSE_MODEL, INGEST_DOC1, "1", true);
        Map<String, Object> updatedDoc = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "1").get("_source");

        assertModelUpdate(originalDoc, updatedDoc, modelId, newModelId);
    }

    private void assertSparseEmbeddings(Map<String, Object> source) {
        List<Map<String, Object>> level1 = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        for (Map<String, Object> nested : level1) {
            Map<String, Object> level2 = (Map<String, Object>) nested.get(LEVEL_2_FIELD);
            Map<String, Double> embeddings = (Map<String, Double>) level2.get(EMBEDDING_FIELD);
            assertNotNull("Embeddings should not be null", embeddings);
            assertTrue("Embedding dimension should be correct", embeddings.size() > 0);
            assertValidSparseEmbeddingValues(embeddings);
        }
    }

    private void assertValidSparseEmbeddingValues(Map<String, Double> embeddings) {
        for (Map.Entry<String, Double> entry : embeddings.entrySet()) {
            Double embedding = entry.getValue();
            assertNotNull("Embedding values should exist", embedding);
        }
    }

    private void assertEmbeddingWithChunks(Map<String, Object> source) {
        List<Map<String, Object>> level = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        for (Map<String, Object> nested : level) {
            Map<String, Object> semanticInfo = (Map<String, Object>) nested.get(LEVEL_2_FIELD);
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) semanticInfo.get(CHUNKS_FIELD);
            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Should have at least one chunk", chunks.size() > 0);

            for (Map<String, Object> chunk : chunks) {
                assertNotNull("Chunk text should not be null", chunk.get(TEXT_FIELD));
                assertNotNull("Chunk embedding should not be null", chunk.get(EMBEDDING_FIELD));
                assertValidSparseEmbeddingValues((Map<String, Double>) chunk.get(EMBEDDING_FIELD));
            }
        }
    }

    private void assertModelUpdate(Map<String, Object> originalDoc, Map<String, Object> updatedDoc, String oldModelId, String newModelId) {
        assertNotEquals("Documents should be different after model update", originalDoc, updatedDoc);
        List<Map<String, Object>> level = (List<Map<String, Object>>) updatedDoc.get(LEVEL_1_FIELD);
        for (Map<String, Object> nested : level) {
            Map<String, Object> semanticInfo = (Map<String, Object>) nested.get(LEVEL_2_FIELD);
            Map<String, Object> model = (Map<String, Object>) semanticInfo.get(MODEL_FIELD);
            assertEquals("Model ID should be updated", newModelId, model.get("id"));
            // Verify model ID has been updated
            assertNotEquals("Model ID should not be the old one", oldModelId, model.get("id"));
        }
    }
}
