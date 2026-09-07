/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.Arrays;

/**
 * Which implementation builds and searches a sparse vector field's index: the JVM one on Lucene
 * postings, or the native nsparse one behind {@link org.opensearch.neuralsearch.jni.NativeLibrary}.
 * Set per field at mapping time and read wherever the two paths diverge.
 */
public enum SparseEngine {
    LUCENE("lucene"),
    NATIVE("native");

    private final String name;
    private final String version = "101";
    private final String extension = ".nsparse";

    SparseEngine(String name) {
        this.name = name;
    }

    /**
     * The engine's wire name in the field mapping.
     */
    public String getName() {
        return name;
    }

    /**
     * Payload layout version of the native engine file. It is part of the file name
     * (see {@link org.opensearch.neuralsearch.sparse.codec.CodecUtils#buildIndexFileName}), so
     * bumping it is what stops a file written by an earlier layout from being read as the current
     * one. Bump it whenever the nsparse index type or its serialized form changes.
     */
    public String version() {
        return version;
    }

    /**
     * File extension of the native engine file.
     */
    public String extension() {
        return extension;
    }

    /**
     * Resolves a mapping value to an engine, case-insensitively.
     *
     * @param name the engine name from the field mapping
     * @return the matching engine, or {@code null} if none matches
     */
    public static SparseEngine fromName(String name) {
        return Arrays.stream(values()).filter(e -> e.name.equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    /** Engine used by a field that does not set one. */
    public static final SparseEngine DEFAULT = LUCENE;
}
