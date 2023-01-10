/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import com.google.common.collect.ImmutableList;

public class RetryUtil {

    private static final int MAX_RETRY = 3;

    private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS = ImmutableList.of(
        NodeNotConnectedException.class,
        NodeDisconnectedException.class
    );

    /**
     *
     * @param e {@link Exception} which is the exception received to check if retryable.
     * @param retryTime {@link int} which is the current retried times.
     * @return {@link boolean} which is the result of if current exception needs retry or not.
     */
    public static boolean shouldRetry(final Exception e, int retryTime) {
        boolean hasRetryException = RETRYABLE_EXCEPTIONS.stream().anyMatch(x -> ExceptionUtils.indexOfThrowable(e, x) != -1);
        return hasRetryException && retryTime < MAX_RETRY;
    }

}
