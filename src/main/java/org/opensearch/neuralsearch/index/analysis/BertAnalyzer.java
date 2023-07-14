/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;

public class BertAnalyzer extends Analyzer {

    public BertAnalyzer() {

    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        BertTokenizer tokenizer = new BertTokenizer();
        return new TokenStreamComponents(tokenizer);
    }
}
