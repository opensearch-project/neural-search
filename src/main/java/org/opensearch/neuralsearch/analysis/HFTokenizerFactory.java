/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

import java.util.Map;

public class HFTokenizerFactory extends AbstractTokenizerFactory {
    private final HuggingFaceTokenizer tokenizer;
    private final Map<String, Float> tokenWeights;

    static private final String DEFAULT_TOKENIZER_ID = "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill";
    static private final String DEFAULT_TOKEN_WEIGHTS_FILE = "query_token_weights.txt";
    static private volatile HuggingFaceTokenizer defaultTokenizer;
    static private volatile Map<String, Float> defaultTokenWeights;

    static public Tokenizer createDefaultTokenizer() {
        // what if throw exception during init?
        if (defaultTokenizer == null) {
            synchronized (HFTokenizerFactory.class) {
                if (defaultTokenizer == null) {
                    defaultTokenizer = DJLUtils.buildHuggingFaceTokenizer(DEFAULT_TOKENIZER_ID);
                    defaultTokenWeights = DJLUtils.fetchTokenWeights(DEFAULT_TOKENIZER_ID, DEFAULT_TOKEN_WEIGHTS_FILE);
                }
            }
        }
        return new HFTokenizer(defaultTokenizer, defaultTokenWeights);
    }

    public HFTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        // For custom tokenizer, the factory is created during IndexModule.newIndexService
        // And can be accessed via indexService.getIndexAnalyzers()
        super(indexSettings, settings, name);
        String tokenizerId = settings.get("tokenizer_id", DEFAULT_TOKENIZER_ID);
        String tokenWeightsFileName = settings.get("token_weights_file", null);
        tokenizer = DJLUtils.buildHuggingFaceTokenizer(tokenizerId);
        if (tokenWeightsFileName != null) {
            tokenWeights = DJLUtils.fetchTokenWeights(tokenizerId, tokenWeightsFileName);
        } else {
            tokenWeights = null;
        }
    }

    @Override
    public Tokenizer create() {
        // the create method will be called for every single analyze request
        return new HFTokenizer(tokenizer, tokenWeights);
    }
}
