/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;

public class TermWeightAnalyzer extends Analyzer {
    public TermWeightAnalyzer() {

    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        TermWeightTokenizer tokenizer = new TermWeightTokenizer();
        return new TokenStreamComponents(tokenizer);
    }
}
