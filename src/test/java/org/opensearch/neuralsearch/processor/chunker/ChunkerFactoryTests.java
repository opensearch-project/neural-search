/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.mockito.Mock;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD;

public class ChunkerFactoryTests extends OpenSearchTestCase {

    @Mock
    private AnalysisRegistry analysisRegistry;

    public void testCreate_FixedTokenLength() {
        Chunker chunker = ChunkerFactory.create(FixedTokenLengthChunker.ALGORITHM_NAME, createChunkParameters());
        assertNotNull(chunker);
        assert (chunker instanceof FixedTokenLengthChunker);
    }

    public void testCreate_Delimiter() {
        Chunker chunker = ChunkerFactory.create(DelimiterChunker.ALGORITHM_NAME, createChunkParameters());
        assertNotNull(chunker);
        assert (chunker instanceof DelimiterChunker);
    }

    public void testCreate_FixedCharLength() {
        Chunker chunker = ChunkerFactory.create(FixedCharLengthChunker.ALGORITHM_NAME, createChunkParameters());
        assertNotNull(chunker);
        assert (chunker instanceof FixedCharLengthChunker);
    }

    public void testCreate_Invalid() {
        String invalidChunkerName = "Invalid Chunker Algorithm";
        assertThrows(NullPointerException.class, () -> ChunkerFactory.create(invalidChunkerName, createChunkParameters()));
    }

    private Map<String, Object> createChunkParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        return parameters;
    }
}
