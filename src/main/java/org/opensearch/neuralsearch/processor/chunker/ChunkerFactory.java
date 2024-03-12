/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.Set;

/**
 * A factory to create different chunking algorithm classes and return all supported chunking algorithms.
 */
public class ChunkerFactory {

    public static Chunker create(String type, Map<String, Object> parameters) {
        switch (type) {
            case FixedTokenLengthChunker.ALGORITHM_NAME:
                return new FixedTokenLengthChunker(parameters);
            case DelimiterChunker.ALGORITHM_NAME:
                return new DelimiterChunker(parameters);
            default:
                throw new IllegalArgumentException(
                    "chunker type [" + type + "] is not supported. Supported chunkers types are " + ChunkerFactory.getAllChunkers()
                );
        }
    }

    public static Set<String> getAllChunkers() {
        return Set.of(FixedTokenLengthChunker.ALGORITHM_NAME, DelimiterChunker.ALGORITHM_NAME);
    }
}
