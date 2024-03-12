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
import java.util.Set;

import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD;

public class ChunkerFactoryTests extends OpenSearchTestCase {

    @Mock
    private AnalysisRegistry analysisRegistry;

    public void testGetAllChunkers() {
        Set<String> expected = Set.of(FixedTokenLengthChunker.ALGORITHM_NAME, DelimiterChunker.ALGORITHM_NAME);
        assertEquals(expected, ChunkerFactory.getAllChunkers());
    }

    public void testCreate_FixedTokenLength() {
        Chunker chunker = ChunkerFactory.create(FixedTokenLengthChunker.ALGORITHM_NAME, createChunkParameters());
        assertNotNull(chunker);
        assertTrue(chunker instanceof FixedTokenLengthChunker);
    }

    public void testCreate_Delimiter() {
        Chunker chunker = ChunkerFactory.create(DelimiterChunker.ALGORITHM_NAME, createChunkParameters());
        assertNotNull(chunker);
        assertTrue(chunker instanceof DelimiterChunker);
    }

    public void testCreate_Invalid() {
        String invalidChunkerType = "Invalid Chunker Type";
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerFactory.create(invalidChunkerType, createChunkParameters())
        );
        assert (illegalArgumentException.getMessage().contains("chunker type [" + invalidChunkerType + "] is not supported."));
    }

    private Map<String, Object> createChunkParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        return parameters;
    }
}
