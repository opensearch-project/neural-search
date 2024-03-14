/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.Set;
import java.util.Locale;

/**
 * A factory to create different chunking algorithm objects and return all supported chunking algorithms.
 */
public class ChunkerFactory {

    public static Chunker create(final String type, final Map<String, Object> parameters) {
        switch (type) {
            case FixedTokenLengthChunker.ALGORITHM_NAME:
                return new FixedTokenLengthChunker(parameters);
            case DelimiterChunker.ALGORITHM_NAME:
                return new DelimiterChunker(parameters);
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "chunking algorithm [%s] is not supported. Supported chunking algorithms are %s",
                        type,
                        ChunkerFactory.getAllChunkers()
                    )
                );
        }
    }

    public static Set<String> getAllChunkers() {
        return Set.of(FixedTokenLengthChunker.ALGORITHM_NAME, DelimiterChunker.ALGORITHM_NAME);
    }
}
