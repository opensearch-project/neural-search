/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import lombok.NonNull;

import java.util.Map;

/**
 * A factory to create chunker validator.
 */
public class ChunkerValidatorFactory {

    final private Map<String, Validator> ALGORITHM_TO_VALIDATOR = Map.of(
        FixedTokenLengthChunker.ALGORITHM_NAME,
        new FixedTokenLengthChunker(),
        DelimiterChunker.ALGORITHM_NAME,
        new DelimiterChunker(),
        FixedCharLengthChunker.ALGORITHM_NAME,
        new FixedCharLengthChunker()
    );

    public Validator getValidator(@NonNull final String algorithm) {
        return ALGORITHM_TO_VALIDATOR.get(algorithm);
    }
}
