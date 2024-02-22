/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;

import org.opensearch.index.analysis.AnalysisRegistry;

import static org.opensearch.action.admin.indices.analyze.TransportAnalyzeAction.analyze;

@Log4j2
public class FixedTokenLengthChunker implements IFieldChunker {

    private static final String TOKEN_LIMIT = "token_limit";
    private static final String OVERLAP_RATE = "overlap_rate";

    private static final String TOKENIZER = "tokenizer";

    private final AnalysisRegistry analysisRegistry;

    public FixedTokenLengthChunker(AnalysisRegistry analysisRegistry) {
        this.analysisRegistry = analysisRegistry;
    }

    private List<String> tokenize(String content, String tokenizer) {
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request();
        analyzeRequest.text(content);
        analyzeRequest.tokenizer(tokenizer);
        try {
            AnalyzeAction.Response analyzeResponse = analyze(analyzeRequest, analysisRegistry, null, 10000);
            List<AnalyzeAction.AnalyzeToken> analyzeTokenList = analyzeResponse.getTokens();
            List<String> tokenList = new ArrayList<>();
            for (AnalyzeAction.AnalyzeToken analyzeToken : analyzeTokenList) {
                tokenList.add(analyzeToken.getTerm());
            }
            return tokenList;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        // parameters has been validated
        int tokenLimit = 500;
        double overlapRate = 0.2;
        String tokenizer = "standard";

        if (parameters.containsKey(TOKEN_LIMIT)) {
            tokenLimit = ((Number) parameters.get(TOKEN_LIMIT)).intValue();
        }
        if (parameters.containsKey(OVERLAP_RATE)) {
            overlapRate = ((Number) parameters.get(OVERLAP_RATE)).doubleValue();
        }
        if (parameters.containsKey(TOKENIZER)) {
            tokenizer = (String) parameters.get(TOKENIZER);
        }

        List<String> tokens = tokenize(content, tokenizer);
        List<String> passages = new ArrayList<>();

        String passage;
        int startToken = 0;
        int overlapTokenNumber = (int) Math.floor(tokenLimit * overlapRate);
        // overlapTokenNumber must be smaller than the token limit
        overlapTokenNumber = Math.min(overlapTokenNumber, tokenLimit - 1);

        while (startToken < tokens.size()) {
            if (startToken + tokenLimit >= tokens.size()) {
                // break the loop when already cover the last token
                passage = String.join(" ", tokens.subList(startToken, tokens.size()));
                passages.add(passage);
                break;
            } else {
                passage = String.join(" ", tokens.subList(startToken, startToken + tokenLimit));
                passages.add(passage);
            }
            startToken += tokenLimit - overlapTokenNumber;
        }
        return passages;
    }

    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(TOKEN_LIMIT)) {
            if (!(parameters.get(TOKEN_LIMIT) instanceof Number)) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + TOKEN_LIMIT + "] cannot be cast to [" + Number.class.getName() + "]"
                );
            }
            if (((Number) parameters.get(TOKEN_LIMIT)).intValue() <= 0) {
                throw new IllegalArgumentException("fixed length parameter [" + TOKEN_LIMIT + "] must be positive");
            }
        }

        if (parameters.containsKey(OVERLAP_RATE)) {
            if (!(parameters.get(OVERLAP_RATE) instanceof Number)) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + OVERLAP_RATE + "] cannot be cast to [" + Number.class.getName() + "]"
                );
            }
            if (((Number) parameters.get(OVERLAP_RATE)).doubleValue() < 0.0
                || ((Number) parameters.get(OVERLAP_RATE)).doubleValue() >= 1.0) {
                throw new IllegalArgumentException(
                    "fixed length parameter [" + OVERLAP_RATE + "] must be between 0 and 1, 1 is not included."
                );
            }
        }

        if (parameters.containsKey(TOKENIZER) && !(parameters.get(TOKENIZER) instanceof String)) {
            throw new IllegalArgumentException(
                "fixed length parameter [" + TOKENIZER + "] cannot be cast to [" + String.class.getName() + "]"
            );
        }
    }
}
