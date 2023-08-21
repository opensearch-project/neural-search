/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.io.IOException;
import java.nio.file.Path;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

public class NeuralSparseQueryProcessor implements SearchRequestProcessor {

    private HuggingFaceTokenizer tokenizer;

    public NeuralSparseQueryProcessor() throws IOException {
        Path path = Path.of(".", "tokenizer.json");
        tokenizer = HuggingFaceTokenizer.builder().optPadding(true).optTokenizerPath(path).build();
    }

    @Override
    public String getDescription() {
        return "NeuralExpansionProcessor";
    }

    @Override
    public String getTag() {
        return "ai, expansion";
    }

    @Override
    public String getType() {
        return "expansion";
    }

    public String mainQueryPhrase(SearchRequest request) {
        return null;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        /*String[] queryInfo = mainQueryPhrase(request);
        String field = queryInfo[0];
        String value = queryInfo[1];

        Encoding enc = tokenizer.encode(value);

        SearchSourceBuilder builder = new SearchSourceBuilder();

        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(null)*/

        return null;
    }

    @Override
    public boolean isIgnoreFailure() {
        return false;
    }

}
