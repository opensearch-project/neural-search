/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analyzer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.opensearch.ml.common.exception.MLException;

import com.google.common.io.CharStreams;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HFModelTokenizer extends Tokenizer {
    public static String ML_PATH = null;
    public static String NAME = "model_tokenizer";

    private final CharTermAttribute termAtt;

    // payload分数属性
    private final PayloadAttribute payloadAtt;

    private HuggingFaceTokenizer tokenizer;

    private Encoding encoding;

    private int tokenIdx = 0;
    private int overflowingIdx = 0;

    public static HuggingFaceTokenizer initializeHFTokenizer(String name) {
        return withDJLContext(() -> HuggingFaceTokenizer.newInstance(name));
    }

    public static HuggingFaceTokenizer initializeHFTokenizerFromConfigString(String configString) {
        return withDJLContext(() -> {
            InputStream inputStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));
            try {
                return HuggingFaceTokenizer.newInstance(inputStream, null);
            } catch (IOException e) {
                throw new IllegalArgumentException("Fail to create tokenizer. " + e.getMessage());
            }
        });
    }

    public static HuggingFaceTokenizer initializeHFTokenizerFromResources() {
        return withDJLContext(() -> {
            try {
                return HuggingFaceTokenizer.newInstance(HFModelTokenizer.class.getResourceAsStream("tokenizer.json"), null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static HuggingFaceTokenizer withDJLContext(Supplier<HuggingFaceTokenizer> tokenizerSupplier) {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<HuggingFaceTokenizer>) () -> {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    System.setProperty("java.library.path", ML_PATH);
                    System.setProperty("DJL_CACHE_DIR", ML_PATH);
                    Thread.currentThread().setContextClassLoader(ai.djl.Model.class.getClassLoader());

                    System.out.println("Current classloader: " + Thread.currentThread().getContextClassLoader());
                    System.out.println("java.library.path: " + System.getProperty("java.library.path"));
                    return tokenizerSupplier.get();
                } finally {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new MLException("error", e);
        }
    }

    public HFModelTokenizer() {
        this(HFModelTokenizer.initializeHFTokenizer("bert-base-uncased"));
    }

    public HFModelTokenizer(HuggingFaceTokenizer tokenizer) {
        termAtt = addAttribute(CharTermAttribute.class);
        payloadAtt = addAttribute(PayloadAttribute.class);
        this.tokenizer = tokenizer;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokenIdx = 0;
        overflowingIdx = -1;
        String inputStr = CharStreams.toString(input);
        encoding = tokenizer.encode(inputStr, false, true);
    }

    @Override
    final public boolean incrementToken() throws IOException {
        // todo: 1. overflowing handle 2. max length of index.analyze.max_token_count 3. other attributes
        clearAttributes();
        Encoding curEncoding = encoding;

        while (tokenIdx < curEncoding.getTokens().length || overflowingIdx < encoding.getOverflowing().length) {
            if (tokenIdx >= curEncoding.getTokens().length) {
                tokenIdx = 0;
                overflowingIdx++;
                if (overflowingIdx < encoding.getOverflowing().length) {
                    curEncoding = encoding.getOverflowing()[overflowingIdx];
                }
                continue;
            }
            termAtt.append(curEncoding.getTokens()[tokenIdx]);
            tokenIdx++;
            return true;
        }

        return false;
        // int intBits = Float.floatToIntBits(10.0f);
        // payloadAtt.setPayload(
        // new BytesRef(new byte[] { (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) })
        // );
    }
}
