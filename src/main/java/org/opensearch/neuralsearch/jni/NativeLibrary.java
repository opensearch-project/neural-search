/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Log4j2
public class NativeLibrary {

    static {
        String loaded = AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            // Report every attempt. Only one variant is ever present, so failures for the
            // other candidates are expected and say nothing; the interesting case is a
            // library that IS on disk but will not link (a missing libomp, say). Rethrowing
            // only the last error reports "not found" for the unsuffixed name and buries the
            // real cause, which makes a broken native install very hard to diagnose.
            List<String> candidates = NativeLibraryCandidates.candidates();
            List<String> failures = new ArrayList<>(candidates.size());
            for (String candidate : candidates) {
                try {
                    System.loadLibrary(candidate);
                    return candidate;
                } catch (UnsatisfiedLinkError e) {
                    failures.add(candidate + ": " + e.getMessage());
                }
            }
            UnsatisfiedLinkError error = new UnsatisfiedLinkError(
                "Could not load the native sparse library. Tried "
                    + candidates
                    + " against java.library.path="
                    + System.getProperty("java.library.path")
                    + ". Attempts: "
                    + String.join("; ", failures)
            );
            log.error(error.getMessage());
            throw error;
        });
        log.info("Loaded library: {}", loaded);
    }

    public static native long initIndex(long numDocs, int dim, Map<String, Object> parameters);

    public static native void insertToIndex(
        long indexAddress,
        int[] ids,
        long indicesAddress,
        long tokensAddress,
        long valueAddress,
        int threadCount
    );

    /**
     * Builds the index at {@code indexAddress} from a CSR file and the id file mapping each of its
     * rows to a doc id, via {@code IDMapIndex::read_csr_and_ids} with mmap residency: the vectors
     * stay borrowed from the CSR file, so both files must outlive {@link #writeIndex}. The index
     * address is not consumed, unlike {@link #insertToIndex}.
     *
     * The index must have been created with {@code idmap: true}, and its delegate must accept mmap
     * residency -- which the scalar-quantized layouts do not.
     *
     * @param indexAddress the index from {@link #initIndex}
     * @param csrPath      filesystem path of the CSR file, in nsparse's native layout
     * @param idPath       filesystem path of the id file, row-aligned with the CSR
     * @param threadCount  build parallelism
     */
    public static native void readCsrAndIdsToIndex(long indexAddress, String csrPath, String idPath, int threadCount);

    public static native void writeIndex(long indexAddress, IndexOutputWrapper output);

    public static native long loadIndex(String indexPath);

    public static native SparseQueryResult[] queryIndex(
        long indexPointer,
        int[] tokens,
        float[] weights,
        int k,
        Map<String, ?> methodParameters
    );

    public static native SparseQueryResult[] queryIndexWithFilter(
        long indexPointer,
        int[] tokens,
        float[] weights,
        int k,
        Map<String, ?> methodParameters,
        long[] filterIds,
        int filterIdsType
    );

    public static native void freeIndex(long indexAddress);

    // common functions
    public static native void transferVectors(long memoryAddresses[], int indices[], int tokens[], float weights[]);

    /**
     * Frees vectors from {@link #transferVectors} that were never handed to
     * {@link #insertToIndex}, which is the only other routine that frees them. Each address is
     * zeroed as it is freed, so a second call frees nothing.
     */
    public static native void freeVectors(long memoryAddresses[]);
}
