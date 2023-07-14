/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.analyzer;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;
import org.opensearch.neuralsearch.index.analysis.BertTokenizer;

@Log4j2
public class BertTokenizerFactory extends AbstractTokenizerFactory {
    public BertTokenizerFactory(IndexSettings indexSettings, Settings settings, String name) {
        super(indexSettings, settings, name);
        // TODO Auto-generated constructor stub
    }

    public static BertTokenizerFactory getBertTokenizerFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        return new BertTokenizerFactory(indexSettings, settings, name);
    }

    @Override
    public Tokenizer create() {
        return new BertTokenizer();
    }

}
