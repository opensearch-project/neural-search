/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.dto;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SemanticFieldInfoTests extends OpenSearchTestCase {
    private static final String CHUNKS_FIELD_NAME = "chunks";
    private static final String CHUNKS_EMBEDDING_FIELD_NAME = "embedding";
    private static final String MODEL_FIELD_NAME = "model";

    private SemanticFieldInfo semanticFieldInfo;

    @Before
    void setup() {
        semanticFieldInfo = new SemanticFieldInfo();
        semanticFieldInfo.setValue("testValue");
        semanticFieldInfo.setModelId("model123");
        semanticFieldInfo.setFullPath("root.path");
        semanticFieldInfo.setSemanticInfoFullPath("root.path_semantic_info");
        semanticFieldInfo.setChunks(List.of("chunk1", "chunk2"));
    }

    public void testGetFullPathForChunks() {
        assertEquals("root.path_semantic_info.chunks", semanticFieldInfo.getFullPathForChunks());
    }

    public void testGetFullPathForEmbedding() {
        assertEquals("root.path_semantic_info.chunks.0.embedding", semanticFieldInfo.getFullPathForEmbedding(0));
    }

    public void testGetFullPathForModelInfo() {
        assertEquals("root.path_semantic_info.model", semanticFieldInfo.getFullPathForModelInfo());
    }
}
