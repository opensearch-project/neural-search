/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A factory to create different chunking algorithm objects and return all supported chunking algorithms.
 */
public final class ChunkerFactory {

    private ChunkerFactory() {} // no instance of this factory class

    private static final ImmutableMap<String, Function<Map<String, Object>, Chunker>> chunkers = ImmutableMap.of(
        FixedTokenLengthChunker.ALGORITHM_NAME,
        FixedTokenLengthChunker::new,
        DelimiterChunker.ALGORITHM_NAME,
        DelimiterChunker::new
    );

    @Getter
    public static Set<String> allChunkerAlgorithms = chunkers.keySet();

    public static Chunker create(final String type, final Map<String, Object> parameters) {
        Function<Map<String, Object>, Chunker> chunkerConstructionFunction = chunkers.get(type);
        // chunkerConstructionFunction is not null because we have validated the type in text chunking processor
        Objects.requireNonNull(chunkerConstructionFunction);
        return chunkerConstructionFunction.apply(parameters);
    }
}
