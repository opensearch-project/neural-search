/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.junit.Before;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

public class MemoryUsageManagerTests extends AbstractSparseTestBase {

    private TestMemoryUsageManager manager;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        manager = new TestMemoryUsageManager();
    }

    public void testGetInstance() {
        MemoryUsageManager instance1 = MemoryUsageManager.getInstance();
        MemoryUsageManager instance2 = MemoryUsageManager.getInstance();
        assertSame(instance1, instance2);
    }

    public void testSetLimitAndOverhead() {
        ByteSizeValue limit = new ByteSizeValue(0);
        manager.setLimitAndOverhead(limit, 1.0);
        assertFalse(manager.getMemoryUsageTracker().record(1));
    }

    public void testSetLimitAndOverheadWithOverHead() {
        ByteSizeValue limit = new ByteSizeValue(10);
        manager.setLimitAndOverhead(limit, 100);
        assertFalse(manager.getMemoryUsageTracker().record(1));
    }

    public void testSetLimitAndOverheadCircuitBreakerTrip() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());
        ByteSizeValue limit = new ByteSizeValue(1000);
        manager.setLimitAndOverhead(limit, 1.0);
        assertFalse(manager.getMemoryUsageTracker().record(1));
    }

    public void testMemoryUsageTrackerInitialization() {
        RamBytesRecorder tracker = manager.getMemoryUsageTracker();
        assertNotNull(tracker);
    }

    private static class TestMemoryUsageManager extends MemoryUsageManager {
        public TestMemoryUsageManager() {
            super();
        }
    }
}
