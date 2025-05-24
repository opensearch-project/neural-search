/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Locale;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseDoubleWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parsePositiveIntegerWithDefault;

/**
 * The implementation {@link Chunker} for fixed string character length algorithm.
 */
public final class CharacterLengthChunker extends Chunker {

    /** The identifier for the fixed string length chunking algorithm. */
    public static final String ALGORITHM_NAME = "character_length";

    /** Field name for specifying the maximum number of characters per chunk. */
    public static final String CHAR_LIMIT_FIELD = "char_limit";

    /** Field name for specifying the overlap rate between consecutive chunks based on character length. */
    public static final String OVERLAP_RATE_FIELD = "overlap_rate";

    // Default values for each non-runtime parameter
    private static final int DEFAULT_CHAR_LIMIT = 500; // Default character limit per chunk
    private static final double DEFAULT_OVERLAP_RATE = 0.0;

    // Parameter restrictions
    private static final double OVERLAP_RATE_LOWER_BOUND = 0.0;
    private static final double OVERLAP_RATE_UPPER_BOUND = 0.5; // Max 50% overlap

    // Parameter values
    private int lengthLimit;
    private double overlapRate;

    /**
     * Constructor that initializes the fixed string length chunker with the specified parameters.
     * @param parameters a map with non-runtime parameters to be parsed
     */
    public CharacterLengthChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    /**
     * Parse the parameters for fixed string length algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. length_limit: the character limit for each chunked passage
     * 2. overlap_rate: the overlapping degree for each chunked passage, indicating how many characters come from the previous passage
     * Here are requirements for non-runtime parameters:
     * 1. length_limit must be a positive integer
     * 2. overlap_rate must be within range [0, 0.5]
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.lengthLimit = parsePositiveIntegerWithDefault(parameters, CHAR_LIMIT_FIELD, DEFAULT_CHAR_LIMIT);
        this.overlapRate = parseDoubleWithDefault(parameters, OVERLAP_RATE_FIELD, DEFAULT_OVERLAP_RATE);

        if (overlapRate < OVERLAP_RATE_LOWER_BOUND || overlapRate > OVERLAP_RATE_UPPER_BOUND) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Parameter [%s] must be between %s and %s, but was %s",
                    OVERLAP_RATE_FIELD,
                    OVERLAP_RATE_LOWER_BOUND,
                    OVERLAP_RATE_UPPER_BOUND,
                    overlapRate
                )
            );
        }
    }

    /**
     * Return the chunked passages for fixed string length algorithm.
     * Throw IllegalArgumentException when runtime parameters are invalid.
     *
     * @param content input string
     * @param runtimeParameters a map for runtime parameters, containing the following runtime parameters:
     * 1. max_chunk_limit: field level max chunk limit
     * 2. chunk_string_count: number of non-empty strings (including itself) which need to be chunked later
     */
    @Override
    public List<String> chunk(final String content, final Map<String, Object> runtimeParameters) {
        int runtimeMaxChunkLimit = parseInteger(runtimeParameters, MAX_CHUNK_LIMIT_FIELD);
        int chunkStringCount = parseInteger(runtimeParameters, CHUNK_STRING_COUNT_FIELD);

        List<String> chunkResult = new ArrayList<>();
        if (content == null || content.isEmpty()) {
            return chunkResult;
        }

        // Should be caught by parsePositiveIntegerWithDefault, but as a safeguard
        if (this.lengthLimit <= 0) {
            chunkResult.add(content);
            return chunkResult;
        }

        int startCharIndex = 0;
        int overlapCharNumber = (int) Math.floor(this.lengthLimit * this.overlapRate);
        // Ensure chunkInterval is positive. lengthLimit is positive. overlapRate is [0, 0.5].
        // So, (lengthLimit - overlapCharNumber) >= 0.5 * lengthLimit, which is > 0 if lengthLimit >= 1.
        int chunkInterval = this.lengthLimit - overlapCharNumber;
        if (chunkInterval <= 0) {
            chunkResult.add(content);
            return chunkResult;
        }

        while (startCharIndex < content.length()) {
            if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                // Add all remaining content as the last chunk
                chunkResult.add(content.substring(startCharIndex));
                break;
            }

            int endPosition = Math.min(startCharIndex + this.lengthLimit, content.length());
            chunkResult.add(content.substring(startCharIndex, endPosition));

            if (endPosition == content.length()) {
                break;
            }

            startCharIndex += chunkInterval;
        }
        return chunkResult;
    }

    @Override
    public String getAlgorithmName() {
        return "";
    }
}
