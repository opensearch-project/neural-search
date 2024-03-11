/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.Set;

import org.opensearch.index.analysis.AnalysisRegistry;

/**
 * A factory to create different chunking algorithm classes and return all supported chunking algorithms.
 */
public class ChunkerFactory {

    public static final String FIXED_TOKEN_LENGTH_ALGORITHM = "fixed_token_length";
    public static final String DELIMITER_ALGORITHM = "delimiter";

    public static Chunker create(String type, Map<String, Object> parameters) {
        switch (type) {
            case FIXED_TOKEN_LENGTH_ALGORITHM:
                return new FixedTokenLengthChunker(parameters);
            case DELIMITER_ALGORITHM:
                return new DelimiterChunker(parameters);
            default:
                throw new IllegalArgumentException(
                    "chunker type [" + type + "] is not supported. Supported chunkers types are " + ChunkerFactory.getAllChunkers()
                );
        }
    }

    public static Set<String> getAllChunkers() {
        return Set.of(FIXED_TOKEN_LENGTH_ALGORITHM, DELIMITER_ALGORITHM);
    }
}
