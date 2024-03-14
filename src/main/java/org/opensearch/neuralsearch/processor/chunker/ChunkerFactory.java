/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Locale;
import java.util.function.Function;

/**
 * A factory to create different chunking algorithm objects and return all supported chunking algorithms.
 */
public class ChunkerFactory {

    private static final ImmutableMap<String, Function<Map<String, Object>, Chunker>> chunkers = ImmutableMap.of(
        FixedTokenLengthChunker.ALGORITHM_NAME,
        FixedTokenLengthChunker::new,
        DelimiterChunker.ALGORITHM_NAME,
        DelimiterChunker::new
    );

    public static Chunker create(final String type, final Map<String, Object> parameters) {
        Function<Map<String, Object>, Chunker> chunkerConstructionFunction = chunkers.get(type);
        if (chunkerConstructionFunction == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Chunking algorithm [%s] is not supported. Supported chunking algorithms are %s",
                    type,
                    chunkers.keySet()
                )
            );
        }
        return chunkerConstructionFunction.apply(parameters);
    }
}
