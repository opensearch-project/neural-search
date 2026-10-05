/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import org.opensearch.OpenSearchException;

/**
 * Thrown when the native sparse library could not be loaded, so a request that needs the native
 * engine cannot be served.
 * <p>
 * An exception rather than the {@link UnsatisfiedLinkError} behind it, and one that does not carry
 * that error as a cause. OpenSearchUncaughtExceptionHandler halts the JVM for every {@code Error}
 * that reaches a thread pool, and ExceptionsHelper.maybeDieOnAnotherThread digs through causes and
 * suppressed exceptions to find one and rethrow it, so a wrapped link error kills the node just as
 * surely as an unwrapped one. Refresh runs the sparse codec, so that halt landed on the first
 * refresh after an ingest, before any response could be sent. Carrying only the message fails the
 * shard or the query instead and leaves the node up; {@link NativeLibrary} logs the error with its
 * stack when the load fails.
 */
public class NativeLibraryUnavailableException extends OpenSearchException {

    public NativeLibraryUnavailableException(String loadFailure) {
        super("the native sparse engine is unavailable: " + loadFailure);
    }
}
