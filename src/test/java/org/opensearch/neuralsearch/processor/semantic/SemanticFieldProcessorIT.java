/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.semantic;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class SemanticFieldProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_WITH_SPARSE_MODEL_WITH_CHUNKING = "semantic_field_sparse_model_with_chunking_index";
    private static final String INDEX_WITH_SPARSE_MODEL_WITH_COMPLEX_CHUNKING = "semantic_field_sparse_model_with_complex_chunking_index";
    private static final String INDEX_WITH_SPARSE_MODEL = "semantic_field_sparse_model_index";
    private static final String INDEX_WITH_SPARSE_MODEL_AND_SPARSE_ENCODING_CONFIG =
        "semantic_field_sparse_model_and_sparse_encoding_config_index";
    private static final String INDEX_WITH_DENSE_MODEL_WITH_CHUNKING = "semantic_field_dense_model_with_chunking_index";
    private static final String INDEX_WITH_DENSE_MODEL = "semantic_field_dense_model_index";
    protected static final String LEVEL_1_FIELD = "products";
    protected static final String LEVEL_2_FIELD = "product_description_semantic_info";
    protected static final String CHUNKS_FIELD = "chunks";
    protected static final String TEXT_FIELD = "text";
    protected static final String EMBEDDING_FIELD = "embedding";
    protected static final String MODEL_FIELD = "model";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/semantic/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/semantic/ingest_doc2.json").toURI()));
    private final String INGEST_DOC4 = Files.readString(Path.of(classLoader.getResource("processor/semantic/ingest_doc4.json").toURI()));
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
        List<Object> embeddings = assertEmbeddings(source);

        // test custom prune type as none
        createIndexWithModelId(
            INDEX_WITH_SPARSE_MODEL_AND_SPARSE_ENCODING_CONFIG,
            "semantic/SemanticIndexMappingsWithSparseEncodingConfig.json",
            modelId
        );
        ingestDocument(INDEX_WITH_SPARSE_MODEL_AND_SPARSE_ENCODING_CONFIG, INGEST_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_WITH_SPARSE_MODEL_AND_SPARSE_ENCODING_CONFIG));
        Map<String, Object> source1 = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL_AND_SPARSE_ENCODING_CONFIG, "1").get(
            "_source"
        );
        List<Object> embeddings1 = assertEmbeddings(source1);

        // verify the sparse embedding has more tokens since we use prune type none and by default
        // we will do max_ratio prune with 0.1f ratio.
        assertTrue(
            "Embedding without prune should have more tokens than the embedding with the default max_ratio 0.1 prune.",
            ((Map) embeddings1.get(0)).size() > ((Map) embeddings.get(0)).size()
        );
    }

    public void testSemanticFieldProcessor_withDenseModel() throws Exception {
        String modelId = prepareModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_DENSE_MODEL, "semantic/SemanticIndexMappingsForDense.json", modelId);

        // Test single document ingestion
        ingestDocument(INDEX_WITH_DENSE_MODEL, INGEST_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_WITH_DENSE_MODEL));

        Map<String, Object> source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL, "1").get("_source");
        assertEmbeddings(source);
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

    public void testSemanticFieldProcessor_withComplexChunking_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        final String targetIndex = INDEX_WITH_SPARSE_MODEL_WITH_COMPLEX_CHUNKING;
        createIndexWithModelId(targetIndex, "semantic/SemanticIndexMappingsWithComplexChunking.json", modelId);

        // Test document with complex chunking configuration
        ingestDocument(targetIndex, INGEST_DOC4, "1");
        assertEquals(1, getDocCount(targetIndex));

        Map<String, Object> source = (Map<String, Object>) getDocById(targetIndex, "1").get("_source");
        assertEmbeddingWithChunks(source);

        List<String> chunkedTexts = extractChunkedTexts(source);
        List<String> expectedChunkedTexts = List.of(
            "This is an example document to be chunked.",
            " The document contains a single paragraph,",
            " two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedChunkedTexts, chunkedTexts);
    }

    private List<String> extractChunkedTexts(Map<String, Object> source) {
        List<String> chunkedTexts = new ArrayList<>();
        List<Map<String, Object>> products = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        for (Map<String, Object> product : products) {
            Map<String, Object> semanticInfo = (Map<String, Object>) product.get(LEVEL_2_FIELD);
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) semanticInfo.get(CHUNKS_FIELD);

            for (Map<String, Object> chunk : chunks) {
                chunkedTexts.add((String) chunk.get(TEXT_FIELD));
            }
        }
        return chunkedTexts;
    }

    public void testSemanticFieldProcessor_withChunking_withDenseModel() throws Exception {
        String modelId = prepareModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, "semantic/SemanticIndexMappingsForDenseWithChunking.json", modelId);

        // Test document with chunking enabled
        ingestDocument(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, INGEST_DOC1, "1");
        assertEquals(1, getDocCount(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING));

        Map<String, Object> source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, "1").get("_source");
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

        assertEmbeddings(doc1Source);
        assertEmbeddings(doc2Source);
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

    public void testSemanticFieldProcessor_batch_withDenseModel() throws Exception {
        String modelId = prepareModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_DENSE_MODEL, "semantic/SemanticIndexMappingsForDense.json", modelId);

        // Test batch document ingestion
        ingestBatchDocumentWithBulk(
            INDEX_WITH_DENSE_MODEL,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_WITH_DENSE_MODEL));
        Map<String, Object> doc1Source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL, "batch_1").get("_source");
        Map<String, Object> doc2Source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL, "batch_2").get("_source");

        assertEmbeddings(doc1Source);
        assertEmbeddings(doc2Source);
    }

    public void testSemanticFieldProcessorWithChunking_batch_withDenseModel() throws Exception {
        String modelId = prepareModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, "semantic/SemanticIndexMappingsForDenseWithChunking.json", modelId);

        // Test batch document ingestion with chunking enabled
        ingestBatchDocumentWithBulk(
            INDEX_WITH_DENSE_MODEL_WITH_CHUNKING,
            "batch_",
            2,
            Collections.emptySet(),
            Collections.emptySet(),
            List.of(INGEST_DOC1, INGEST_DOC2),
            BULK_ITEM_TEMPLATE
        );
        assertEquals(2, getDocCount(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING));

        Map<String, Object> doc1Source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, "batch_1").get("_source");
        Map<String, Object> doc2Source = (Map<String, Object>) getDocById(INDEX_WITH_DENSE_MODEL_WITH_CHUNKING, "batch_2").get("_source");

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

    public void testReuseExitEmbedding_withSparseModel() throws Exception {
        String modelId = prepareSparseEncodingModel();
        loadModel(modelId);
        createIndexWithModelId(INDEX_WITH_SPARSE_MODEL, "semantic/SemanticIndexMappingsSkipExistingEmbedding.json", modelId);

        // Initial ingestion
        ingestDocument(INDEX_WITH_SPARSE_MODEL, INGEST_DOC1, "1");
        Map<String, Object> originalDoc = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "1").get("_source");
        assertEmbeddings(originalDoc);

        // Re-ingest and verify changes
        ingestDocument(INDEX_WITH_SPARSE_MODEL, INGEST_DOC1, "1", true);
        Map<String, Object> updatedDoc = (Map<String, Object>) getDocById(INDEX_WITH_SPARSE_MODEL, "1").get("_source");
        assertEmbeddings(updatedDoc);
    }

    private List<Object> assertEmbeddings(Map<String, Object> source) {
        List<Object> embeddings = new ArrayList<>();
        List<Map<String, Object>> level1 = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        for (Map<String, Object> nested : level1) {
            Map<String, Object> level2 = (Map<String, Object>) nested.get(LEVEL_2_FIELD);
            doAssertEmbedding(level2.get(EMBEDDING_FIELD), embeddings);
        }
        return embeddings;
    }

    private void doAssertEmbedding(Object embedding, List<Object> embeddings) {
        assertNotNull("Embedding should not be null", embedding);
        if (embedding instanceof Map<?, ?>) {
            Map<String, Double> embeddingMap = (Map<String, Double>) embedding;
            embeddings.add(embeddingMap);
            assertValidSparseEmbeddingValues(embeddingMap);
        } else if (embedding instanceof List<?>) {
            List<Float> embeddingList = (List<Float>) embedding;
            embeddings.add(embeddingList);
            assertTrue("Embedding dimension should be correct", embeddingList.size() == 768);
        } else {
            fail("Embeddings are neither a map for sparse model nor a list for dense model.");
        }
    }

    private void assertValidSparseEmbeddingValues(Map<String, Double> embeddings) {
        for (Map.Entry<String, Double> entry : embeddings.entrySet()) {
            Double embedding = entry.getValue();
            assertNotNull("Embedding values should exist", embedding);
        }
    }

    private List<List<Object>> assertEmbeddingWithChunks(Map<String, Object> source) {
        List<List<Object>> embeddingsList = new ArrayList<>();
        List<Map<String, Object>> level = (List<Map<String, Object>>) source.get(LEVEL_1_FIELD);
        for (Map<String, Object> nested : level) {
            List<Object> embeddings = new ArrayList<>();
            Map<String, Object> semanticInfo = (Map<String, Object>) nested.get(LEVEL_2_FIELD);
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) semanticInfo.get(CHUNKS_FIELD);
            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Should have at least one chunk", chunks.size() > 0);

            for (Map<String, Object> chunk : chunks) {
                assertNotNull("Chunk text should not be null", chunk.get(TEXT_FIELD));
                doAssertEmbedding(chunk.get(EMBEDDING_FIELD), embeddings);
            }

            embeddingsList.add(embeddings);
        }

        return embeddingsList;
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
