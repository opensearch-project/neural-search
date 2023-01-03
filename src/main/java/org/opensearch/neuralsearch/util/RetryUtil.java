/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.List;

public class RetryUtil {

    private static final int MAX_RETRY = 3;

    private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS = ImmutableList.of(NodeNotConnectedException.class,
        NodeDisconnectedException.class);

    public static boolean shouldRetry(final Exception e, int retryTime) {
        boolean hasRetryException = RETRYABLE_EXCEPTIONS.stream().anyMatch(x -> ExceptionUtils.indexOfThrowable(e, x) != -1);
        return hasRetryException && retryTime < MAX_RETRY;
    }

}
