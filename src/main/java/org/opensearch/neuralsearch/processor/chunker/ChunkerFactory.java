/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A factory to create different chunking algorithm objects.
 */
public final class ChunkerFactory {

    private ChunkerFactory() {} // no instance of this factory class

    private static final Map<String, Function<Map<String, Object>, Chunker>> CHUNKERS_CONSTRUCTORS = ImmutableMap.of(
        FixedTokenLengthChunker.ALGORITHM_NAME,
        FixedTokenLengthChunker::new,
        FixedStringLengthChunker.ALGORITHM_NAME,
        FixedStringLengthChunker::new,
        DelimiterChunker.ALGORITHM_NAME,
        DelimiterChunker::new
    );

    /** Set of supported chunker algorithm types */
    public static Set<String> CHUNKER_ALGORITHMS = CHUNKERS_CONSTRUCTORS.keySet();

    /**
     * Creates a new Chunker instance based on the specified type and parameters.
     *
     * @param type the type of chunker to create
     * @param parameters configuration parameters for the chunker
     * @return a new Chunker instance configured with the given parameters
     */
    public static Chunker create(final String type, final Map<String, Object> parameters) {
        Function<Map<String, Object>, Chunker> chunkerConstructionFunction = CHUNKERS_CONSTRUCTORS.get(type);
        // chunkerConstructionFunction is not null because we have validated the type in text chunking processor
        Objects.requireNonNull(chunkerConstructionFunction);
        return chunkerConstructionFunction.apply(parameters);
    }
}
