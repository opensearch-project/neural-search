/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Locale;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import static org.opensearch.action.admin.indices.analyze.TransportAnalyzeAction.analyze;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseStringWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseDoubleWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parsePositiveIntegerWithDefault;

/**
 * The implementation {@link Chunker} for fixed token length algorithm.
 */
public final class FixedTokenLengthChunker implements Chunker {

    /** The identifier for the fixed token length chunking algorithm. */
    public static final String ALGORITHM_NAME = "fixed_token_length";

    /** Field name for the analysis registry configuration parameter. */
    public static final String ANALYSIS_REGISTRY_FIELD = "analysis_registry";

    /** Field name for specifying the maximum number of tokens per chunk. */
    public static final String TOKEN_LIMIT_FIELD = "token_limit";

    /** Field name for specifying the overlap rate between consecutive chunks. */
    public static final String OVERLAP_RATE_FIELD = "overlap_rate";

    /** Field name for specifying the maximum token count allowed in the input text. */
    public static final String MAX_TOKEN_COUNT_FIELD = "max_token_count";

    /** Field name for specifying the tokenizer to be used for text analysis. */
    public static final String TOKENIZER_FIELD = "tokenizer";

    // default values for each non-runtime parameter
    private static final int DEFAULT_TOKEN_LIMIT = 384;
    private static final double DEFAULT_OVERLAP_RATE = 0.0;
    private static final String DEFAULT_TOKENIZER = "standard";

    // parameter restrictions
    private static final double OVERLAP_RATE_LOWER_BOUND = 0.0;
    private static final double OVERLAP_RATE_UPPER_BOUND = 0.5;
    private static final Set<String> WORD_TOKENIZERS = Set.of(
        "standard",
        "letter",
        "lowercase",
        "whitespace",
        "uax_url_email",
        "classic",
        "thai"
    );

    // parameter value
    private int tokenLimit;
    private String tokenizer;
    private double overlapRate;
    private final AnalysisRegistry analysisRegistry;

    /**
     * Constructor that initializes the fixed token length chunker with the specified parameters.
     * @param parameters a map with non-runtime parameters to be parsed
     */
    public FixedTokenLengthChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
        this.analysisRegistry = (AnalysisRegistry) parameters.get(ANALYSIS_REGISTRY_FIELD);
    }

    /**
     * Parse the parameters for fixed token length algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. tokenizer: the <a href="https://opensearch.org/docs/latest/analyzers/tokenizers/index/">word tokenizer</a> in opensearch
     * 2. token_limit: the token limit for each chunked passage
     * 3. overlap_rate: the overlapping degree for each chunked passage, indicating how many token comes from the previous passage
     * 4. max_chunk_limit processor level max chunk level
     * Here are requirements for non-runtime parameters:
     * 1. token_limit must be a positive integer
     * 2. overlap_rate must be within range [0, 0.5]
     * 3. tokenizer must be a word tokenizer
     *
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.tokenLimit = parsePositiveIntegerWithDefault(parameters, TOKEN_LIMIT_FIELD, DEFAULT_TOKEN_LIMIT);
        this.overlapRate = parseDoubleWithDefault(parameters, OVERLAP_RATE_FIELD, DEFAULT_OVERLAP_RATE);
        this.tokenizer = parseStringWithDefault(parameters, TOKENIZER_FIELD, DEFAULT_TOKENIZER);
        if (overlapRate < OVERLAP_RATE_LOWER_BOUND || overlapRate > OVERLAP_RATE_UPPER_BOUND) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Parameter [%s] must be between %s and %s",
                    OVERLAP_RATE_FIELD,
                    OVERLAP_RATE_LOWER_BOUND,
                    OVERLAP_RATE_UPPER_BOUND
                )
            );
        }
        if (!WORD_TOKENIZERS.contains(tokenizer)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Tokenizer [%s] is not supported for [%s] algorithm. Supported tokenizers are %s",
                    tokenizer,
                    ALGORITHM_NAME,
                    WORD_TOKENIZERS
                )
            );
        }
    }

    /**
     * Return the chunked passages for fixed token length algorithm.
     * Throw IllegalArgumentException when runtime parameters are invalid.
     *
     * @param content input string
     * @param runtimeParameters a map for runtime parameters, containing the following runtime parameters:
     * 1. max_token_count the max token limit for the tokenizer
     * 2. max_chunk_limit field level max chunk limit
     * 3. chunk_string_count number of non-empty strings (including itself) which need to be chunked later
     */
    @Override
    public List<String> chunk(final String content, final Map<String, Object> runtimeParameters) {
        int maxTokenCount = parseInteger(runtimeParameters, MAX_TOKEN_COUNT_FIELD);
        int runtimeMaxChunkLimit = parseInteger(runtimeParameters, MAX_CHUNK_LIMIT_FIELD);
        int chunkStringCount = parseInteger(runtimeParameters, CHUNK_STRING_COUNT_FIELD);

        List<AnalyzeToken> tokens = tokenize(content, tokenizer, maxTokenCount);
        List<String> chunkResult = new ArrayList<>();

        int startTokenIndex = 0;
        int startContentPosition, endContentPosition;
        int overlapTokenNumber = (int) Math.floor(tokenLimit * overlapRate);

        while (startTokenIndex < tokens.size()) {
            if (startTokenIndex == 0) {
                // include all characters till the start if no previous passage
                startContentPosition = 0;
            } else {
                startContentPosition = tokens.get(startTokenIndex).getStartOffset();
            }
            if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                // include all characters till the end if exceeds max chunk limit
                chunkResult.add(content.substring(startContentPosition));
                break;
            }
            if (startTokenIndex + tokenLimit >= tokens.size()) {
                // include all characters till the end if no next passage
                endContentPosition = content.length();
                chunkResult.add(content.substring(startContentPosition, endContentPosition));
                break;
            } else {
                // include gap characters between two passages
                endContentPosition = tokens.get(startTokenIndex + tokenLimit).getStartOffset();
                chunkResult.add(content.substring(startContentPosition, endContentPosition));
            }
            startTokenIndex += tokenLimit - overlapTokenNumber;
        }
        return chunkResult;
    }

    private List<AnalyzeToken> tokenize(final String content, final String tokenizer, final int maxTokenCount) {
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request();
        analyzeRequest.text(content);
        analyzeRequest.tokenizer(tokenizer);
        try {
            AnalyzeAction.Response analyzeResponse = analyze(analyzeRequest, analysisRegistry, null, maxTokenCount);
            return analyzeResponse.getTokens();
        } catch (Exception e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "analyzer %s throws exception: %s", tokenizer, e.getMessage()), e);
        }
    }

    @Override
    public String getAlgorithmName() {
        return ALGORITHM_NAME;
    }
}
