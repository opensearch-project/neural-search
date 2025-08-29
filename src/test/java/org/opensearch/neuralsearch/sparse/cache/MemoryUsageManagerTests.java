/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.junit.After;
import org.junit.Before;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

public class MemoryUsageManagerTests extends AbstractSparseTestBase {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MemoryUsageManager.getInstance().setLimit(new ByteSizeValue(Integer.MAX_VALUE));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        MemoryUsageManager.getInstance().setLimit(new ByteSizeValue(Integer.MAX_VALUE));
    }

    public void testGetInstance() {
        MemoryUsageManager instance1 = MemoryUsageManager.getInstance();
        MemoryUsageManager instance2 = MemoryUsageManager.getInstance();
        assertSame(instance1, instance2);
        assertNotNull(instance1.getMemoryUsageTracker());
    }

    public void testSetLimit() {
        MemoryUsageManager manager = MemoryUsageManager.getInstance();
        ByteSizeValue limit = new ByteSizeValue(0);
        manager.setLimit(limit);
        assertFalse(manager.getMemoryUsageTracker().record(1));
    }

    public void testSetLimitCircuitBreakerTrip() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());
        MemoryUsageManager manager = MemoryUsageManager.getInstance();
        ByteSizeValue limit = new ByteSizeValue(1000);
        manager.setLimit(limit);
        assertFalse(manager.getMemoryUsageTracker().record(1));
    }

    public void testMemoryUsageTrackerInitialization() {
        MemoryUsageManager manager = MemoryUsageManager.getInstance();
        RamBytesRecorder tracker = manager.getMemoryUsageTracker();
        assertNotNull(tracker);
    }
}
