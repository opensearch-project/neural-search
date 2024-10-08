/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analyzer;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.training.util.ProgressBar;
import org.opensearch.test.OpenSearchTestCase;

public class HFTokenizerTests extends OpenSearchTestCase {
    public void testEncoding() {
        int totalIt = 10000;
        ProgressBar pb = new ProgressBar("message", totalIt);
        String s = "hello world sdlfgjnus ".repeat(10);
        HFModelTokenizerFactory.createDefault();
        HuggingFaceTokenizer tokenizer = HFModelTokenizerFactory.getDefaultTokenizer();
        for (int i = 0; i < totalIt; i++) {
            tokenizer.encode(s);
            pb.increment(1);
        }
    }
}
