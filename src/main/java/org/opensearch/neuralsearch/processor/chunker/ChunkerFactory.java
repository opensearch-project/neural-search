/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.opensearch.index.analysis.AnalysisRegistry;

import java.util.Set;

/**
 * A factory to create different chunking algorithm classes and return all supported chunking algorithms.
 */
public class ChunkerFactory {

    public static final String FIXED_LENGTH_ALGORITHM = "fix_length";
    public static final String DELIMITER_ALGORITHM = "delimiter";

    public static FieldChunker create(String type, AnalysisRegistry analysisRegistry) {
        switch (type) {
            case FIXED_LENGTH_ALGORITHM:
                return new FixedTokenLengthChunker(analysisRegistry);
            case DELIMITER_ALGORITHM:
                return new DelimiterChunker();
            default:
                throw new IllegalArgumentException(
                    "chunker type ["
                        + type
                        + "] is not supported. Supported chunkers types are ["
                        + FIXED_LENGTH_ALGORITHM
                        + ", "
                        + DELIMITER_ALGORITHM
                        + "]"
                );
        }
    }

    public static Set<String> getAllChunkers() {
        return Set.of(FIXED_LENGTH_ALGORITHM, DELIMITER_ALGORITHM);
    }

}
