/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.analyzer;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;
import org.opensearch.neuralsearch.index.analysis.TermWeightTokenizer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class TermWeightTokenizerFactory extends AbstractTokenizerFactory {
    public TermWeightTokenizerFactory(
        IndexSettings indexSettings,
        Settings settings,
        String name
    ) {
        super(indexSettings, settings, name);
    }

    public static TermWeightTokenizerFactory getTermWeightTokenizerFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        return new TermWeightTokenizerFactory(indexSettings, settings, name);
    }

    @Override
    public Tokenizer create() {
        return new TermWeightTokenizer();
    }

    
}
