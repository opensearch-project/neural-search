/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Locale;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseDoubleWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parsePositiveIntegerWithDefault;

/**
 * The implementation {@link Chunker} for fixed character length algorithm.
 */
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public final class FixedCharLengthChunker extends Chunker {

    /** The identifier for the fixed character length chunking algorithm. */
    public static final String ALGORITHM_NAME = "fixed_char_length";

    /** Field name for specifying the maximum number of characters per chunk. */
    public static final String CHAR_LIMIT_FIELD = "char_limit";

    /** Field name for specifying the overlap rate between consecutive chunks based on fixed character length. */
    public static final String OVERLAP_RATE_FIELD = "overlap_rate";

    // Default values for each non-runtime parameter
    private static final int DEFAULT_CHAR_LIMIT = 2048; // Default character limit per chunk (512 tokens * 4 chars)
    private static final double DEFAULT_OVERLAP_RATE = 0.0;

    // Parameter restrictions
    private static final double OVERLAP_RATE_LOWER_BOUND = 0.0;
    private static final double OVERLAP_RATE_UPPER_BOUND = 0.5; // Max 50% overlap

    // Parameter values
    private int charLimit;
    private double overlapRate;

    /**
     * Constructor that initializes the fixed character length chunker with the specified parameters.
     * @param parameters a map with non-runtime parameters to be parsed
     */
    public FixedCharLengthChunker(final Map<String, Object> parameters) {
        parse(parameters);
    }

    /**
     * Parse the parameters for fixed character length algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. char_limit: the character limit for each chunked passage
     * 2. overlap_rate: the overlapping degree for each chunked passage, indicating how many characters come from the previous passage
     * Here are requirements for non-runtime parameters:
     * 1. char_limit must be a positive integer
     * 2. overlap_rate must be within range [0, 0.5]
     */
    @Override
    public void parse(Map<String, Object> parameters) throws IllegalArgumentException {
        super.parse(parameters);
        this.charLimit = parsePositiveIntegerWithDefault(parameters, CHAR_LIMIT_FIELD, DEFAULT_CHAR_LIMIT);
        final double overlapRate = parseDoubleWithDefault(parameters, OVERLAP_RATE_FIELD, DEFAULT_OVERLAP_RATE);
        validateOverlapRate(overlapRate);
        this.overlapRate = overlapRate;
    }

    /**
     * Return the chunked passages for fixed character length algorithm.
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

        int startCharIndex = 0;
        int overlapCharNumber = (int) Math.floor(this.charLimit * this.overlapRate);
        // Ensure `chunkInterval` is positive. charLimit is positive. overlapRate is [0, 0.5].
        // So, (charLimit - overlapCharNumber) >= 0.5 * charLimit, which is always > 0 if charLimit >= 1.
        int chunkInterval = this.charLimit - overlapCharNumber;

        while (startCharIndex < content.length()) {
            if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                chunkResult.add(content.substring(startCharIndex));
                break;
            }

            int endPosition;
            // Check if the current chunk will extend to or past the end of the content
            if (startCharIndex + this.charLimit >= content.length()) {
                endPosition = content.length(); // Ensure chunk goes to the very end
                chunkResult.add(content.substring(startCharIndex, endPosition));
                break;
            } else {
                endPosition = startCharIndex + this.charLimit;
                chunkResult.add(content.substring(startCharIndex, endPosition));
            }

            startCharIndex += chunkInterval;
        }

        return chunkResult;
    }

    @Override
    public String getAlgorithmName() {
        return ALGORITHM_NAME;
    }

    /**
     * Validate the parameters for the FixedCharLengthChunker
     * @param parameters parameters for the FixedCharLengthChunker
     */
    @Override
    public void validate(Map<String, Object> parameters) {
        super.validate(parameters);
        parsePositiveIntegerWithDefault(parameters, CHAR_LIMIT_FIELD, DEFAULT_CHAR_LIMIT);
        final Double overlapRate = parseDoubleWithDefault(parameters, OVERLAP_RATE_FIELD, DEFAULT_OVERLAP_RATE);
        validateOverlapRate(overlapRate);
    }

    private void validateOverlapRate(final Double overlapRate) {
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
}
