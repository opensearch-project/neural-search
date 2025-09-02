/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Locale;

/**
 * Manages circuit breaker functionality for neural search operations.
 * This class provides a centralized way to track and manage memory usage for neural search
 * operations. It helps prevent out-of-memory situations by tracking memory allocations and
 * notify when memory usage exceeds configured thresholds.
 * The circuit breaker will be set upon plugin initialization.
 */
@Log4j2
public class CircuitBreakerManager {

    private static CircuitBreaker circuitBreaker;

    public synchronized static void setCircuitBreaker(@NonNull CircuitBreaker circuitBreaker) {
        CircuitBreakerManager.circuitBreaker = circuitBreaker;
    }

    /**
     * Adds memory usage for neural search operations
     *
     * @param bytes The number of bytes to add to the circuit breaker
     * @param label A label to identify the operation in case of circuit breaking
     * @return false when the limit would be exceeded
     */
    public static boolean addMemoryUsage(long bytes, String label) {
        try {
            circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
        } catch (CircuitBreakingException circuitBreakingException) {
            log.debug(
                String.format(
                    Locale.ROOT,
                    "Cannot insert data into cache due to circuit breaker exception: %s",
                    circuitBreakingException.getMessage()
                )
            );
            return false;
        }
        return true;
    }

    /**
     * Updates memory usage for neural search operations without throwing exception
     *
     * @param bytes The number of bytes to add to the circuit breaker
     */
    public static void addWithoutBreaking(long bytes) {
        circuitBreaker.addWithoutBreaking(bytes);
    }

    /**
     * Decreases the tracked memory usage
     *
     * @param bytes The number of bytes to release from the circuit breaker
     */
    public static void releaseBytes(long bytes) {
        circuitBreaker.addWithoutBreaking(-bytes);
    }

    /**
     * Set the circuit breaker memory limit and overhead
     *
     * @param limit The limit of the circuit breaker
     * @param overhead The overhead of the circuit breaker
     */
    public static void setLimitAndOverhead(ByteSizeValue limit, double overhead) {
        circuitBreaker.setLimitAndOverhead(limit.getBytes(), overhead);
    }
}
