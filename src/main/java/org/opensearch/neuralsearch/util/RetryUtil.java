/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.List;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import com.google.common.collect.ImmutableList;
import org.opensearch.common.Randomness;

@Log4j2
public class RetryUtil {

    private static final int DEFAULT_MAX_RETRY = 3;
    private static final long DEFAULT_BASE_DELAY_MS = 500;
    private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS = ImmutableList.of(
        NodeNotConnectedException.class,
        NodeDisconnectedException.class
    );

    /**
     * Handle retry or failure based on the exception and retry time
     * @param e Exception
     * @param retryTime Retry time
     * @param retryAction Action to retry
     * @param listener Listener to handle success or failure
     */
    public static void handleRetryOrFailure(Exception e, int retryTime, Runnable retryAction, ActionListener<?> listener) {
        if (shouldRetry(e, retryTime)) {
            long backoffTime = calculateBackoffTime(retryTime);
            log.warn("Retrying connection for ML inference due to [{}] after [{}ms]", e.getMessage(), backoffTime, e);
            try {
                Thread.sleep(backoffTime);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                listener.onFailure(interruptedException);
                return;
            }
            retryAction.run();
        } else {
            listener.onFailure(e);
        }
    }

    private static boolean shouldRetry(final Exception e, int retryTime) {
        return isRetryableException(e) && retryTime < DEFAULT_MAX_RETRY;
    }

    private static boolean isRetryableException(final Exception e) {
        return RETRYABLE_EXCEPTIONS.stream().anyMatch(x -> ExceptionUtils.indexOfThrowable(e, x) != -1);
    }

    private static long calculateBackoffTime(int retryTime) {
        long backoffTime = DEFAULT_BASE_DELAY_MS * (1L << retryTime); // Exponential backoff
        long jitter = Randomness.get().nextLong(10, 50); // Add jitter between 10ms and 50ms
        return backoffTime + jitter;
    }
}
