/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class ClusterTrainingRunningTests extends AbstractSparseTestBase {
    private ThreadPool threadPool;
    private ExecutorService executorService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        when(threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME)).thenReturn(executorService);
    }

    public void testInitialize_setsThreadPool() {
        ClusterTrainingRunning.initialize(threadPool);

        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();
        assertNotNull(instance);
    }

    public void testGetInstance_returnsSameInstance() {
        ClusterTrainingRunning instance1 = ClusterTrainingRunning.getInstance();
        ClusterTrainingRunning instance2 = ClusterTrainingRunning.getInstance();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertSame(instance1, instance2);
    }

    public void testGetExecutor_returnsCorrectExecutor() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

        ExecutorService result = (ExecutorService) instance.getExecutor();

        assertEquals(executorService, result);
        verify(threadPool, times(1)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
    }

    public void testRun_executesRunnable() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();
        Runnable runnable = mock(Runnable.class);

        instance.run(runnable);

        verify(threadPool, times(1)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(runnable);
    }

    public void testSubmit_returnsCorrectFuture() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();
        Callable<String> callable = () -> "test result";
        Future<String> expectedFuture = mock(Future.class);
        when(executorService.submit(callable)).thenReturn(expectedFuture);

        Future<String> result = instance.submit(callable);

        assertEquals(expectedFuture, result);
        verify(threadPool, times(1)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
        verify(executorService, times(1)).submit(callable);
    }

    public void testSubmit_withDifferentCallableTypes() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

        // Test with Integer callable
        Callable<Integer> intCallable = () -> 42;
        Future<Integer> intFuture = mock(Future.class);
        when(executorService.submit(intCallable)).thenReturn(intFuture);

        Future<Integer> intResult = instance.submit(intCallable);

        assertEquals(intFuture, intResult);
        verify(executorService, times(1)).submit(intCallable);
    }

    public void testThreadPoolName_hasCorrectValue() {
        assertEquals("cluster_training_thread_pool", ClusterTrainingRunning.THREAD_POOL_NAME);
    }

    public void testMultipleRuns_callsExecutorMultipleTimes() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();
        Runnable runnable1 = mock(Runnable.class);
        Runnable runnable2 = mock(Runnable.class);

        instance.run(runnable1);
        instance.run(runnable2);

        verify(threadPool, times(2)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(runnable1);
        verify(executorService, times(1)).execute(runnable2);
    }

    public void testGetExecutor_afterInitialization_returnsValidExecutor() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

        ExecutorService executor1 = (ExecutorService) instance.getExecutor();
        ExecutorService executor2 = (ExecutorService) instance.getExecutor();

        assertNotNull(executor1);
        assertNotNull(executor2);
        assertEquals(executor1, executor2);
        verify(threadPool, times(2)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
    }

    public void testSubmit_withNullCallable_passesToExecutor() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

        instance.submit(null);

        verify(threadPool, times(1)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
        verify(executorService, times(1)).submit((Callable<Object>) null);
    }

    public void testRun_withNullRunnable_passesToExecutor() {
        ClusterTrainingRunning.initialize(threadPool);
        ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

        instance.run(null);

        verify(threadPool, times(1)).executor(ClusterTrainingRunning.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(null);
    }
}
