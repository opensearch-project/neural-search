/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import org.opensearch.ExceptionsHelper;
import org.opensearch.test.OpenSearchTestCase;

/**
 * What keeps a broken native install from killing the node: the failure has to reach OpenSearch as
 * an exception with no {@code Error} anywhere in it. OpenSearchUncaughtExceptionHandler halts the
 * JVM for every Error that escapes onto a thread pool, and
 * {@link ExceptionsHelper#maybeDieOnAnotherThread} rethrows one found nested inside an exception —
 * and the sparse codec runs on the refresh thread, so an UnsatisfiedLinkError there took the node
 * down before any response could be written.
 */
public class NativeLibraryUnavailableExceptionTests extends OpenSearchTestCase {

    public void testIsNotAnError() {
        Throwable thrown = new NativeLibraryUnavailableException("no opensearch_neuralsearch_nsparse in java.library.path");

        assertFalse("an Error here halts the node instead of failing the request", thrown instanceof Error);
        assertTrue(thrown instanceof RuntimeException);
    }

    /**
     * The check OpenSearch itself makes before deciding to die. It walks causes and suppressed
     * exceptions, so attaching the UnsatisfiedLinkError as a cause would be enough to lose the node.
     */
    public void testHidesNoErrorFromMaybeDieOnAnotherThread() {
        Throwable thrown = new NativeLibraryUnavailableException("no opensearch_neuralsearch_nsparse in java.library.path");

        assertTrue("nothing in this exception may be an Error", ExceptionsHelper.maybeError(thrown).isEmpty());
    }

    /** The load attempt lists every name and path it tried, which is the whole diagnosis. */
    public void testKeepsTheLoadFailureDetail() {
        String loadFailure = "tried [a, b] against java.library.path=/usr/lib";

        NativeLibraryUnavailableException thrown = new NativeLibraryUnavailableException(loadFailure);

        assertTrue(thrown.getMessage(), thrown.getMessage().contains(loadFailure));
    }
}
