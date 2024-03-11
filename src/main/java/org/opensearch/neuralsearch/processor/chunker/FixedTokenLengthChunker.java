/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.math.NumberUtils;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;

import static org.opensearch.action.admin.indices.analyze.TransportAnalyzeAction.analyze;

/**
 * The implementation {@link Chunker} for fixed token length algorithm.
 */
@Log4j2
public class FixedTokenLengthChunker implements Chunker {

    public static final String TOKEN_LIMIT_FIELD = "token_limit";
    public static final String OVERLAP_RATE_FIELD = "overlap_rate";
    public static final String MAX_TOKEN_COUNT_FIELD = "max_token_count";
    public static final String ANALYSIS_REGISTRY_FIELD = "analysis_registry";
    public static final String TOKENIZER_FIELD = "tokenizer";

    // default values for each parameter
    private static final int DEFAULT_TOKEN_LIMIT = 384;
    private static final BigDecimal DEFAULT_OVERLAP_RATE = new BigDecimal("0");
    private static final int DEFAULT_MAX_TOKEN_COUNT = 10000;
    private static final String DEFAULT_TOKENIZER = "standard";
    private static final BigDecimal OVERLAP_RATE_UPPER_BOUND = new BigDecimal("0.5");

    private int tokenLimit = DEFAULT_TOKEN_LIMIT;
    private BigDecimal overlapRate = DEFAULT_OVERLAP_RATE;
    private String tokenizer = DEFAULT_TOKENIZER;
    private AnalysisRegistry analysisRegistry;

    public FixedTokenLengthChunker(Map<String, Object> parameters) {
        validateParameters(parameters);
    }

    /**
     * Validate and parse the parameters for fixed token length algorithm,
     * will throw IllegalArgumentException when parameters are invalid
     *
     * @param parameters a map containing parameters, containing the following parameters:
     * 1. tokenizer: the <a href="https://opensearch.org/docs/latest/analyzers/tokenizers/index/">analyzer tokenizer</a> in opensearch
     * 2. token_limit: the token limit for each chunked passage
     * 3. overlap_rate: the overlapping degree for each chunked passage, indicating how many token comes from the previous passage
     * 4. max_token_count: the max token limit for the tokenizer
     * Here are requirements for parameters:
     * max_token_count and token_limit should be a positive integer
     * overlap_rate should be within range [0, 0.5]
     * tokenizer should be string
     */
    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(TOKEN_LIMIT_FIELD)) {
            String tokenLimitString = parameters.get(TOKEN_LIMIT_FIELD).toString();
            if (!(NumberUtils.isParsable(tokenLimitString))) {
                throw new IllegalArgumentException(
                        "fixed length parameter [" + TOKEN_LIMIT_FIELD + "] cannot be cast to [" + Number.class.getName() + "]"
                );
            }
            this.tokenLimit = NumberUtils.createInteger(tokenLimitString);
            if (tokenLimit <= 0) {
                throw new IllegalArgumentException("fixed length parameter [" + TOKEN_LIMIT_FIELD + "] must be positive");
            }
        }

        if (parameters.containsKey(MAX_TOKEN_COUNT_FIELD)) {
            String maxTokenCountString = parameters.get(MAX_TOKEN_COUNT_FIELD).toString();
            if (!(NumberUtils.isParsable(maxTokenCountString))) {
                throw new IllegalArgumentException(
                        "fixed length parameter [" + MAX_TOKEN_COUNT_FIELD + "] cannot be cast to [" + Number.class.getName() + "]"
                );
            }
            this.maxTokenCount = NumberUtils.createInteger(maxTokenCountString);
            if (maxTokenCount <= 0) {
                throw new IllegalArgumentException("fixed length parameter [" + MAX_TOKEN_COUNT_FIELD + "] must be positive");
            }
        }

        if (parameters.containsKey(OVERLAP_RATE_FIELD)) {
            String overlapRateString = parameters.get(OVERLAP_RATE_FIELD).toString();
            if (!(NumberUtils.isParsable(overlapRateString))) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + OVERLAP_RATE_FIELD + "] cannot be cast to [" + Number.class.getName() + "]"
                );
            }
            this.overlapRate = new BigDecimal(overlapRateString);
            if (overlapRate.compareTo(BigDecimal.ZERO) < 0 || overlapRate.compareTo(OVERLAP_RATE_UPPER_BOUND) > 0) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + OVERLAP_RATE_FIELD + "] must be between 0 and " + OVERLAP_RATE_UPPER_BOUND
                );
            }
        }

        if (parameters.containsKey(TOKENIZER_FIELD)) {
            if (!(parameters.get(TOKENIZER_FIELD) instanceof String)) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + TOKENIZER_FIELD + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }
            this.tokenizer = parameters.get(TOKENIZER_FIELD).toString();
        }
    }

    /**
     * Return the chunked passages for fixed token length algorithm
     *
     * @param content input string
     */
    @Override
    public List<String> chunk(String content, Map<String, Object> runtimeParameters) {
        List<AnalyzeToken> tokens = tokenize(content, tokenizer, maxTokenCount);
        List<String> passages = new ArrayList<>();

        int startTokenIndex = 0, endTokenIndex;
        int startContentPosition, endContentPosition;
        BigDecimal overlapTokenNumberBigDecimal = overlapRate.multiply(new BigDecimal(String.valueOf(tokenLimit)))
            .setScale(0, RoundingMode.DOWN);
        int overlapTokenNumber = overlapTokenNumberBigDecimal.intValue();

        while (startTokenIndex < tokens.size()) {
            endTokenIndex = Math.min(tokens.size(), startTokenIndex + tokenLimit) - 1;
            startContentPosition = tokens.get(startTokenIndex).getStartOffset();
            endContentPosition = tokens.get(endTokenIndex).getEndOffset();
            passages.add(content.substring(startContentPosition, endContentPosition));
            if (startTokenIndex + tokenLimit >= tokens.size()) {
                break;
            }
            startTokenIndex += tokenLimit - overlapTokenNumber;
        }
        return passages;
    }

    private List<AnalyzeToken> tokenize(String content, String tokenizer, int maxTokenCount) {
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request();
        analyzeRequest.text(content);
        analyzeRequest.tokenizer(tokenizer);
        try {
            AnalyzeAction.Response analyzeResponse = analyze(analyzeRequest, analysisRegistry, null, maxTokenCount);
            return analyzeResponse.getTokens();
        } catch (IOException e) {
            throw new RuntimeException("Fixed token length algorithm meet with exception: " + e);
        }
    }
}
