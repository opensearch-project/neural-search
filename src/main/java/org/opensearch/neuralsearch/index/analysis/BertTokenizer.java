/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index.analysis;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import lombok.extern.log4j.Log4j2;

import com.google.common.io.CharStreams;

@Log4j2
public class BertTokenizer extends Tokenizer {

    // 词元文本属性
    private final CharTermAttribute termAtt;

    // payload分数属性
    private final PayloadAttribute payloadAtt;

    private static HuggingFaceTokenizer tokenizer;

    private static String DJL_CACHE_DIR;

    private List<String> tokens;

    private int cursor = 0;

    private static Void initalizeHuggingFaaceTokenizer() {
        try {
            /*System.setProperty("PYTORCH_PRECXX11", "true");
            
            // DJL will read "/usr/java/packages/lib" if don't set "java.library.path". That will throw
            // access denied exception
            System.setProperty("java.library.path", DJL_CACHE_DIR);
            System.setProperty("ai.djl.pytorch.num_interop_threads", "1");
            System.setProperty("ai.djl.pytorch.num_threads", "1");*/
            
            System.setProperty("DJL_CACHE_DIR", DJL_CACHE_DIR);
            Thread.currentThread().setContextClassLoader(ai.djl.Model.class.getClassLoader());
            Path path = Path.of("/Users/zhichaog/Desktop/repos/neural-search", "tokenizer.json");
            tokenizer = HuggingFaceTokenizer.builder().optPadding(true).optTokenizerPath(path).build();
        } catch (Exception e) {
            log.error("Tail to create tokenizer, ", e);
        }
        return null;
    }

    public BertTokenizer() {
        termAtt = addAttribute(CharTermAttribute.class);
        payloadAtt = addAttribute(PayloadAttribute.class);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        cursor = 0;
        try {
            String inputStr = CharStreams.toString(input);
            tokens = tokenizer.tokenize(inputStr);
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @Override
    final public boolean incrementToken() throws IOException {
        clearAttributes();

        if (cursor >= tokens.size()) {
            return false;
        }
        
        termAtt.append(tokens.get(cursor));
        int intBits = Float.floatToIntBits(1.0f);
        payloadAtt.setPayload(
            new BytesRef(new byte[] { (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) })
        );
        cursor++;

        return true;
    }

}
