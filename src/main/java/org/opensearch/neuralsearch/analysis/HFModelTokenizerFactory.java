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
import java.util.Objects;

public class HFModelTokenizerFactory extends AbstractTokenizerFactory {
    private final HuggingFaceTokenizer tokenizer;
    private final Map<String, Float> tokenWeights;

    /**
     * Atomically loads the HF tokenizer in a lazy fashion once the outer class accesses the static final set the first time.;
     */
    private static class DefaultTokenizerHolder {
        static final HuggingFaceTokenizer TOKENIZER;
        static final Map<String, Float> TOKEN_WEIGHTS;
        static private final String DEFAULT_TOKENIZER_ID = "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill";
        static private final String DEFAULT_TOKEN_WEIGHTS_FILE = "query_token_weights.txt";

        static {
            try {
                TOKENIZER = DJLUtils.buildHuggingFaceTokenizer(DEFAULT_TOKENIZER_ID);
                TOKEN_WEIGHTS = DJLUtils.fetchTokenWeights(DEFAULT_TOKENIZER_ID, DEFAULT_TOKEN_WEIGHTS_FILE);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize default hf_model_tokenizer", e);
            }
        }
    }

    static public Tokenizer createDefault() {
        return new HFModelTokenizer(DefaultTokenizerHolder.TOKENIZER, DefaultTokenizerHolder.TOKEN_WEIGHTS);
    }

    public HFModelTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        // For custom tokenizer, the factory is created during IndexModule.newIndexService
        // And can be accessed via indexService.getIndexAnalyzers()
        super(indexSettings, settings, name);
        String tokenizerId = settings.get("tokenizer_id", null);
        Objects.requireNonNull(tokenizerId, "tokenizer_id is required");
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
        return new HFModelTokenizer(tokenizer, tokenWeights);
    }
}
