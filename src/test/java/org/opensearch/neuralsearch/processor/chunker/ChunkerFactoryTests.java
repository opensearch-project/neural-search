/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.mockito.Mock;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ChunkerFactoryTests extends OpenSearchTestCase {

    @Mock
    private AnalysisRegistry analysisRegistry;

    public void testGetAllChunkers() {
        Set<String> expected = Set.of(ChunkerFactory.FIXED_LENGTH_ALGORITHM, ChunkerFactory.DELIMITER_ALGORITHM);
        assertEquals(expected, ChunkerFactory.getAllChunkers());
    }

    public void testCreate_FixedTokenLength() {
        FieldChunker chunker = ChunkerFactory.create(ChunkerFactory.FIXED_LENGTH_ALGORITHM, analysisRegistry);
        assertNotNull(chunker);
        assertTrue(chunker instanceof FixedTokenLengthChunker);
    }

    public void testCreate_Delimiter() {
        FieldChunker chunker = ChunkerFactory.create(ChunkerFactory.DELIMITER_ALGORITHM, analysisRegistry);
        assertNotNull(chunker);
        assertTrue(chunker instanceof DelimiterChunker);
    }

    public void testCreate_Invalid() {
        String invalidChunkerType = "Invalid Chunker Type";
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ChunkerFactory.create(invalidChunkerType, analysisRegistry)
        );
        assert (illegalArgumentException.getMessage().contains("chunker type [" + invalidChunkerType + "] is not supported."));
    }
}
