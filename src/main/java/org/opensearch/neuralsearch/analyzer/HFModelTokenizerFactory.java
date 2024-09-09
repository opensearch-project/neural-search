/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analyzer;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

public class HFModelTokenizerFactory extends AbstractTokenizerFactory {
    private String modelName;
    private HuggingFaceTokenizer tokenizer;
    static private HuggingFaceTokenizer defaultTokenizer;

    static public Tokenizer createDefault() {
        // what if throw exception during init?
        if (defaultTokenizer == null) {
            synchronized (HFModelTokenizerFactory.class) {
                if (defaultTokenizer == null) {
                    defaultTokenizer = HFModelTokenizer.initializeHFTokenizer("bert-base-uncased");
                    // defaultTokenizer = HFModelTokenizer.initializeHFTokenizer(
                    // "amazon/neural-sparse/opensearch-neural-sparse-tokenizer-v1",
                    // "1.0.1",
                    // MLModelFormat.TORCH_SCRIPT
                    // );
                    // defaultTokenizer = HFModelTokenizer.initializeHFTokenizerFromResources();
                }
            }
        }
        return new HFModelTokenizer(defaultTokenizer);
    }

    public HFModelTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        // todo: require additional settings?
        super(indexSettings, settings, name);
        String configString = settings.get("tokenizer_config", null);
        if (StringUtils.isNotBlank(configString)) {
            tokenizer = HFModelTokenizer.initializeHFTokenizerFromConfigString(configString);
        } else {
            tokenizer = HFModelTokenizer.initializeHFTokenizer("bert-base-uncased");
        }
    }

    @Override
    public Tokenizer create() {
        return new HFModelTokenizer(tokenizer);
    }
}
