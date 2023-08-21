/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index.analysis;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import com.google.common.io.CharStreams;

public class TermWeightTokenizer extends Tokenizer {
    // 词元文本属性
    private final CharTermAttribute termAtt;
    // payload分数属性
    private final PayloadAttribute payloadAtt;

    private ArrayList<String> tokens;
    private ArrayList<Float> weights;
    private int cursor = 0;

    public TermWeightTokenizer() {
        termAtt = addAttribute(CharTermAttribute.class);
        payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens = new ArrayList<>();
        weights = new ArrayList<>();
        try {
            String inputStr = CharStreams.toString(input);
            String[] eles = inputStr.split("\\|");

            if (eles != null) {
                for (int i = 0; i < eles.length - 1; i += 2) {
                    tokens.add(eles[i]);
                    weights.add(Float.parseFloat(eles[i + 1]));
                }
            }
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @Override
    final public boolean incrementToken() throws IOException {
        clearAttributes();

        if (tokens.size() == 0 || cursor >= tokens.size()) {
            return false;
        }

        termAtt.append(tokens.get(cursor));
        int intBits = Float.floatToIntBits(weights.get(cursor));
        payloadAtt.setPayload(
            new BytesRef(new byte[] { (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) })
        );
        cursor++;

        return true;
    }
}
