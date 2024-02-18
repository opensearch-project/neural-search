/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.client.node.NodeClient;

public class FixedTokenLengthChunker implements IFieldChunker {

    private static final String TOKEN_LIMIT = "token_limit";
    private static final String OVERLAP_RATE = "overlap_rate";

    private static final String TOKENIZER = "tokenizer";

    private final NodeClient nodeClient;

    public FixedTokenLengthChunker(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    private List<String> tokenize(String content, String tokenizer) {
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request();
        analyzeRequest.text(content);
        analyzeRequest.tokenizer(tokenizer);
        AnalyzeAction.Response analyzeResponse = nodeClient.admin().indices().analyze(analyzeRequest).actionGet();
        List<AnalyzeAction.AnalyzeToken> analyzeTokenList = analyzeResponse.getTokens();
        List<String> tokenList = new ArrayList<>();
        for (AnalyzeAction.AnalyzeToken analyzeToken : analyzeTokenList) {
            tokenList.add(analyzeToken.getTerm());
        }
        return tokenList;
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        // parameters has been validated
        int tokenLimit = 500;
        double overlapRate = 0.2;
        String tokenizer = "standard";

        if (parameters.containsKey(TOKEN_LIMIT)) {
            Number tokenLimitParam = (Number) parameters.get(TOKEN_LIMIT);
            tokenLimit = (int) tokenLimitParam.intValue();
        }
        if (parameters.containsKey(OVERLAP_RATE)) {
            Number overlapRateParam = (Number) parameters.get(OVERLAP_RATE);
            overlapRate = overlapRateParam.doubleValue();
            overlapRate = Math.min(1.0, overlapRate);
            overlapRate = Math.max(0.0, overlapRate);
        }
        if (parameters.containsKey(TOKENIZER)) {
            tokenizer = (String) parameters.get(TOKENIZER);
        }

        List<String> tokens = tokenize(content, tokenizer);
        List<String> passages = new ArrayList<>();

        int startToken = 0;
        int overlapTokenNumber = (int) Math.floor(tokenLimit * overlapRate);
        // overlapTokenNumber must be smaller than the token limit
        overlapTokenNumber = Math.min(overlapTokenNumber, tokenLimit - 1);

        while (startToken < tokens.size()) {
            if (startToken + tokenLimit >= tokens.size()) {
                // break the loop when already cover the last token
                String passage = String.join(" ", tokens.subList(startToken, startToken + tokenLimit));
                passages.add(passage);
                break;
            } else {
                String passage = String.join(" ", tokens.subList(startToken, startToken + tokenLimit));
                passages.add(passage);
            }
            startToken += (tokenLimit - overlapTokenNumber);
        }
        return passages;
    }

    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(TOKEN_LIMIT) && !(parameters.get(TOKEN_LIMIT) instanceof Number)) {
            throw new IllegalArgumentException(
                "fixed length parameter [" + TOKEN_LIMIT + "] cannot be cast to [" + Number.class.getName() + "]"
            );
        }

        if (parameters.containsKey(OVERLAP_RATE) && !(parameters.get(OVERLAP_RATE) instanceof Number)) {
            throw new IllegalArgumentException(
                "fixed length parameter [" + OVERLAP_RATE + "] cannot be cast to [" + Number.class.getName() + "]"
            );
        }

        if (parameters.containsKey(TOKENIZER) && !(parameters.get(TOKENIZER) instanceof String)) {
            throw new IllegalArgumentException(
                "fixed length parameter [" + TOKENIZER + "] cannot be cast to [" + String.class.getName() + "]"
            );
        }
    }
}
