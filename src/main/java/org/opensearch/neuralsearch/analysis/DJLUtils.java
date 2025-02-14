/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class DJLUtils {
    static private Path ML_CACHE_PATH;
    static private String ML_CACHE_DIR_NAME = "ml_cache";
    static private String HUGGING_FACE_BASE_URL = "https://huggingface.co/";
    static private String HUGGING_FACE_RESOLVE_PATH = "resolve/main/";

    static public void buildDJLCachePath(Path opensearchDataFolder) {
        // the logic to build cache path is consistent with ml-commons plugin
        // see
        // https://github.com/opensearch-project/ml-commons/blob/14b971214c488aa3f4ab150d1a6cc379df1758be/ml-algorithms/src/main/java/org/opensearch/ml/engine/MLEngine.java#L53
        ML_CACHE_PATH = opensearchDataFolder.resolve(ML_CACHE_DIR_NAME);
    }

    public static <T> T withDJLContext(Callable<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                System.setProperty("java.library.path", ML_CACHE_PATH.toAbsolutePath().toString());
                System.setProperty("DJL_CACHE_DIR", ML_CACHE_PATH.toAbsolutePath().toString());
                Thread.currentThread().setContextClassLoader(ai.djl.Model.class.getClassLoader());

                return action.call();
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        });
    }

    public static HuggingFaceTokenizer buildHuggingFaceTokenizer(String tokenizerId) {
        try {
            return withDJLContext(() -> HuggingFaceTokenizer.newInstance(tokenizerId));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Failed to initialize Hugging Face tokenizer. " + e);
        }
    }

    public static Map<String, Float> parseInputStreamToTokenWeights(InputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            Map<String, Float> tokenWeights = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid line in token weights file: " + line);
                }
                String token = parts[0];
                float weight = Float.parseFloat(parts[1]);
                tokenWeights.put(token, weight);
            }
            return tokenWeights;
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse token weights file. " + e);
        }
    }

    public static Map<String, Float> fetchTokenWeights(String tokenizerId, String fileName) {
        Map<String, Float> tokenWeights = new HashMap<>();
        String url = HUGGING_FACE_BASE_URL + tokenizerId + "/" + HUGGING_FACE_RESOLVE_PATH + fileName;

        InputStream inputStream = null;
        try {
            inputStream = withDJLContext(() -> Utils.openUrl(url));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Failed to download file from " + url, e);
        }

        return parseInputStreamToTokenWeights(inputStream);
    }
}
