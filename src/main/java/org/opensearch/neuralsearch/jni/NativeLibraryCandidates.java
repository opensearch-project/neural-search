/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * The set of shared-library names to try when loading the native sparse engine.
 * <p>
 * Kept separate from {@link NativeLibrary} on purpose: NativeLibrary's static
 * initializer loads the library, so touching any of its static state pulls the
 * shared object in. This class holds the naming policy with no side effects, so
 * it can be asserted against jni/cmake/init-nsparse.cmake in a plain unit test.
 */
final class NativeLibraryCandidates {

    static final String LIBRARY_NAME = "opensearch_neuralsearch_nsparse";

    /**
     * jni/cmake/init-nsparse.cmake appends the SIMD variant it built to the library
     * name, so the file on disk is rarely the bare LIBRARY_NAME. A distribution build
     * (scripts/build.sh) produces every variant its architecture can use, so which
     * files exist says nothing about which ones this CPU can execute -- each suffix
     * is paired with the check that decides whether it is safe to load. The generic
     * build carries no requirement and stays last.
     * <p>
     * Must stay in sync with the {@code string(PREPEND LIB_EXT ...)} calls in
     * init-nsparse.cmake; NativeLibraryContractTests pins the pairing.
     */
    private static final Map<String, BooleanSupplier> SIMD_REQUIREMENTS = requirements();

    /** Suffixes in load order, most specific first, regardless of what the host supports. */
    static final List<String> SIMD_SUFFIXES = List.copyOf(SIMD_REQUIREMENTS.keySet());

    private static Map<String, BooleanSupplier> requirements() {
        Map<String, BooleanSupplier> byPriority = new LinkedHashMap<>();
        byPriority.put("_avx512", NativeCpuFeatures::supportsAvx512);
        byPriority.put("_avx2", NativeCpuFeatures::supportsAvx2);
        byPriority.put("_sve", NativeCpuFeatures::supportsSve);
        byPriority.put("", () -> true);
        return Collections.unmodifiableMap(byPriority);
    }

    /**
     * Library names to try, in order: the variants this CPU can execute, most
     * specific first, ending with the generic build.
     */
    static List<String> candidates() {
        return SIMD_REQUIREMENTS.entrySet()
            .stream()
            .filter(entry -> entry.getValue().getAsBoolean())
            .map(entry -> LIBRARY_NAME + entry.getKey())
            .collect(Collectors.toUnmodifiableList());
    }

    /** Every name a build can produce, whether or not this CPU can run it. */
    static List<String> allCandidates() {
        return SIMD_SUFFIXES.stream().map(suffix -> LIBRARY_NAME + suffix).collect(Collectors.toUnmodifiableList());
    }

    private NativeLibraryCandidates() {}
}
