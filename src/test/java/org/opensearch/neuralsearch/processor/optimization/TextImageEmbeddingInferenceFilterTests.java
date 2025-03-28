/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import org.junit.Before;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TextImageEmbeddingInferenceFilterTests extends OpenSearchTestCase {

    private Map<String, String> knnMap;
    private Map<String, Object> existingSourceAndMetadataMap;
    private IngestDocument ingestDocument;
    private TextImageEmbeddingInferenceFilter textImageEmbeddingInferenceFilter;
    private final String embeddingField = "vector_embedding";

    @Before
    public void setup() {
        textImageEmbeddingInferenceFilter = new TextImageEmbeddingInferenceFilter();
        knnMap = new HashMap<>();
        existingSourceAndMetadataMap = new HashMap<>();
        existingSourceAndMetadataMap.put("image_description", "orange desk");
        existingSourceAndMetadataMap.put("image_binary", "base64_of_orange_desk_image");
        existingSourceAndMetadataMap.put(embeddingField, Arrays.asList(0.1, 0.2, 0.3));
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
    }

    public void test_filterAndCopyExistingEmbeddings_TextAndImageUnchanged_ShouldCopyEmbedding() {
        knnMap.put("image_description", "orange desk");
        knnMap.put("image_binary", "base64_of_orange_desk_image");

        Map<String, String> result = textImageEmbeddingInferenceFilter.filterAndCopyExistingEmbeddings(
            ingestDocument,
            existingSourceAndMetadataMap,
            knnMap,
            embeddingField
        );
        assertTrue(result.isEmpty());
        assertEquals(existingSourceAndMetadataMap.get(embeddingField), ingestDocument.getSourceAndMetadata().get(embeddingField));
    }

    public void test_filterAndCopyExistingEmbeddings_TextChanged_ShouldNotCopyEmbedding() {
        knnMap.put("image_description", "blue desk");
        knnMap.put("image_binary", "base64_of_orange_desk_image");

        Map<String, String> result = textImageEmbeddingInferenceFilter.filterAndCopyExistingEmbeddings(
            ingestDocument,
            existingSourceAndMetadataMap,
            knnMap,
            embeddingField
        );
        assertEquals(result, knnMap);
        assertNull(ingestDocument.getSourceAndMetadata().get(embeddingField));
    }

    public void test_filterAndCopyExistingEmbeddings_ImageChanged_ShouldNotCopyEmbedding() {
        knnMap.put("image_description", "orange desk");
        knnMap.put("image_binary", "base64_of_blue_desk_image");

        Map<String, String> result = textImageEmbeddingInferenceFilter.filterAndCopyExistingEmbeddings(
            ingestDocument,
            existingSourceAndMetadataMap,
            knnMap,
            embeddingField
        );
        assertEquals(result, knnMap);
        assertNull(ingestDocument.getSourceAndMetadata().get(embeddingField));
    }

    public void test_filterAndCopyExistingEmbeddings_EmbeddingDoesNotExist_ShouldNotCopyEmbedding() {
        knnMap.put("image_description", "orange desk");
        knnMap.put("image_binary", "base64_of_blue_desk_image");
        existingSourceAndMetadataMap.remove(embeddingField);
        Map<String, String> result = textImageEmbeddingInferenceFilter.filterAndCopyExistingEmbeddings(
            ingestDocument,
            existingSourceAndMetadataMap,
            knnMap,
            embeddingField
        );
        assertEquals(result, knnMap);
        assertNull(ingestDocument.getSourceAndMetadata().get(embeddingField));
    }
}
