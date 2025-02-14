/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import com.google.common.io.CharStreams;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import org.apache.lucene.util.BytesRef;

public class HFModelTokenizer extends Tokenizer {
    public static final String NAME = "hf_model_tokenizer";
    private static final Float DEFAULT_TOKEN_WEIGHT = 1.0f;

    private final CharTermAttribute termAtt;
    private final PayloadAttribute payloadAtt;
    private final OffsetAttribute offsetAtt;
    private final HuggingFaceTokenizer tokenizer;
    private final Map<String, Float> tokenWeights;

    private Encoding encoding;
    private int tokenIdx = 0;
    private int overflowingIdx = 0;

    public HFModelTokenizer(HuggingFaceTokenizer huggingFaceTokenizer) {
        this(huggingFaceTokenizer, null);
    }

    public HFModelTokenizer(HuggingFaceTokenizer huggingFaceTokenizer, Map<String, Float> weights) {
        termAtt = addAttribute(CharTermAttribute.class);
        offsetAtt = addAttribute(OffsetAttribute.class);
        if (Objects.nonNull(weights)) {
            payloadAtt = addAttribute(PayloadAttribute.class);
        } else {
            payloadAtt = null;
        }
        tokenizer = huggingFaceTokenizer;
        tokenWeights = weights;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokenIdx = 0;
        overflowingIdx = -1;
        String inputStr = CharStreams.toString(input);
        encoding = tokenizer.encode(inputStr, false, true);
    }

    private static boolean isLastTokenInEncodingSegment(int idx, Encoding encodingSegment) {
        return idx >= encodingSegment.getTokens().length || encodingSegment.getAttentionMask()[idx] == 0;
    }

    public static byte[] floatToBytes(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }

    public static float bytesToFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    @Override
    final public boolean incrementToken() throws IOException {
        clearAttributes();
        Encoding curEncoding = overflowingIdx == -1 ? encoding : encoding.getOverflowing()[overflowingIdx];

        while (!isLastTokenInEncodingSegment(tokenIdx, curEncoding) || overflowingIdx < encoding.getOverflowing().length) {
            if (isLastTokenInEncodingSegment(tokenIdx, curEncoding)) {
                // reset cur segment, go to the next segment
                // until overflowingIdx = encoding.getOverflowing().length
                tokenIdx = 0;
                overflowingIdx++;
                if (overflowingIdx >= encoding.getOverflowing().length) {
                    return false;
                }
                curEncoding = encoding.getOverflowing()[overflowingIdx];
            } else {
                termAtt.append(curEncoding.getTokens()[tokenIdx]);
                offsetAtt.setOffset(
                    curEncoding.getCharTokenSpans()[tokenIdx].getStart(),
                    curEncoding.getCharTokenSpans()[tokenIdx].getEnd()
                );
                if (Objects.nonNull(tokenWeights)) {
                    // for neural sparse query, write the token weight to payload field
                    payloadAtt.setPayload(
                        new BytesRef(floatToBytes(tokenWeights.getOrDefault(curEncoding.getTokens()[tokenIdx], DEFAULT_TOKEN_WEIGHT)))
                    );
                }
                tokenIdx++;
                return true;
            }
        }

        return false;
    }
}
