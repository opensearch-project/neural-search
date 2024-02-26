/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ChunkerFactoryTests extends OpenSearchTestCase {

    private AnalysisRegistry registry;

    public void testGetAllChunkers() {
        Set<String> expected = Set.of(ChunkerFactory.FIXED_LENGTH_ALGORITHM, ChunkerFactory.DELIMITER_ALGORITHM);
        assertEquals(expected, ChunkerFactory.getAllChunkers());
    }

    public void testCreate_FixedTokenLength() {
        IFieldChunker chunker = ChunkerFactory.create(ChunkerFactory.FIXED_LENGTH_ALGORITHM, registry);
        assertNotNull(chunker);
        assertTrue(chunker instanceof FixedTokenLengthChunker);
    }

    public void testCreate_Delimiter() {
        IFieldChunker chunker = ChunkerFactory.create(ChunkerFactory.DELIMITER_ALGORITHM, registry);
        assertNotNull(chunker);
        assertTrue(chunker instanceof DelimiterChunker);
    }

    public void testCreate_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> ChunkerFactory.create("Invalid Chunker Type", registry));
    }
}
