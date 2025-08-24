/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.mockito.Mockito;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class CircuitBreakerManagerTests extends AbstractSparseTestBase {

    private static final long TEST_BYTE_SIZE = 1000L;

    /**
     * Test that addWithoutBreaking correctly calls the circuit breaker's addWithoutBreaking method
     * with the provided number of bytes.
     */
    public void test_addWithoutBreaking_callsCircuitBreakerCorrectly() {
        CircuitBreakerManager.addWithoutBreaking(TEST_BYTE_SIZE);

        Mockito.verify(mockedCircuitBreaker).addWithoutBreaking(TEST_BYTE_SIZE);
    }

    /**
     * Tests that the releaseBytes method correctly decreases the tracked memory usage
     * by calling addWithoutBreaking with a negative value on the circuit breaker.
     */
    public void test_releaseBytes_decreasesTrackedMemory() {
        CircuitBreakerManager.releaseBytes(TEST_BYTE_SIZE);

        verify(mockedCircuitBreaker).addWithoutBreaking(-TEST_BYTE_SIZE);
    }

    /**
     * Test that setCircuitBreaker method throws NullPointerException when null is passed.
     * This test verifies the @NonNull annotation on the method parameter.
     */
    public void test_setCircuitBreaker_nullInput() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { CircuitBreakerManager.setCircuitBreaker(null); });
        assertEquals("circuitBreaker is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test case for setCircuitBreaker method.
     * This test verifies that the circuit breaker is correctly set in the CircuitBreakerManager.
     */
    public void test_setCircuitBreaker_setsCircuitBreakerCorrectly() {
        CircuitBreaker newMockedCircuitBreaker = mock(CircuitBreaker.class);

        CircuitBreakerManager.setCircuitBreaker(newMockedCircuitBreaker);

        // Note: As the circuit breaker instance is declared private in the manager,
        // We verify the circuit breaker is correctly set by calling other methods
        CircuitBreakerManager.releaseBytes(TEST_BYTE_SIZE);
        verify(newMockedCircuitBreaker).addWithoutBreaking(-TEST_BYTE_SIZE);
    }

    /**
     * Tests that the setLimitAndOverhead method correctly sets the limit and overhead
     * on the circuit breaker with the provided values.
     */
    public void test_setLimitAndOverhead_setsCorrectLimitAndOverhead() {
        double overhead = 1.5;
        ByteSizeValue limit = new ByteSizeValue(TEST_BYTE_SIZE);

        CircuitBreakerManager.setLimitAndOverhead(limit, overhead);

        verify(mockedCircuitBreaker).setLimitAndOverhead(limit.getBytes(), overhead);
    }

    /**
     * Tests the setLimitAndOverhead method with a negative overhead value.
     * This is an edge case that should be handled by the CircuitBreaker implementation.
     */
    public void test_setLimitAndOverhead_with_negative_overhead() {
        ByteSizeValue limit = new ByteSizeValue(1000);
        double negativeOverhead = -0.1;

        CircuitBreakerManager.setLimitAndOverhead(limit, negativeOverhead);

        verify(mockedCircuitBreaker).setLimitAndOverhead(limit.getBytes(), negativeOverhead);
    }

    /**
     * Tests the addMemoryUsage method when a CircuitBreakingException is thrown.
     * This test verifies that the method returns true when the circuit breaker
     * throws an exception, indicating that the memory limit would be exceeded.
     */
    public void test_addMemoryUsage_returnsFalse_whenCircuitBreakingException() throws Exception {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        boolean result = CircuitBreakerManager.addMemoryUsage(TEST_BYTE_SIZE, "test");

        verify(mockedCircuitBreaker).addEstimateBytesAndMaybeBreak(TEST_BYTE_SIZE, "test");
        assertFalse(result);
    }

    /**
     * Tests that addMemoryUsage returns false when no exception is thrown.
     * This test mocks the CircuitBreaker does not throw an exception when addEstimateBytesAndMaybeBreak is called.
     */
    public void test_addMemoryUsage_returnsTrue_whenNoException() {
        when(mockedCircuitBreaker.addEstimateBytesAndMaybeBreak(anyLong(), anyString()))
            .thenReturn(0d);

        boolean result = CircuitBreakerManager.addMemoryUsage(TEST_BYTE_SIZE, "TestLabel");

        assertTrue(result);
    }

}
