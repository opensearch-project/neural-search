/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.mockito.ArgumentCaptor;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class ClusterTrainingExecutorTests extends AbstractSparseTestBase {
    private ThreadPool threadPool;
    private ExecutorService executorService;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        when(threadPool.executor(SparseConstants.THREAD_POOL_NAME)).thenReturn(executorService);
    }

    public void testGetExecutor_returnsCorrectExecutor() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        ExecutorService result = (ExecutorService) ClusterTrainingExecutor.getInstance().getExecutor();

        assertEquals(executorService, result);
        verify(threadPool, times(1)).executor(SparseConstants.THREAD_POOL_NAME);
    }

    public void testRun_executesRunnable() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        Runnable runnable = mock(Runnable.class);

        ClusterTrainingExecutor.getInstance().run(runnable);

        verify(threadPool, times(1)).executor(SparseConstants.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(runnable);
    }

    public void testSubmit_returnsCorrectFuture() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        Callable<String> callable = () -> "test result";
        Future<String> expectedFuture = mock(Future.class);
        when(executorService.submit(callable)).thenReturn(expectedFuture);

        Future<String> result = ClusterTrainingExecutor.getInstance().submit(callable);

        assertEquals(expectedFuture, result);
        verify(threadPool, times(1)).executor(SparseConstants.THREAD_POOL_NAME);
        verify(executorService, times(1)).submit(callable);
    }

    public void testSubmit_withDifferentCallableTypes() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);

        // Test with Integer callable
        Callable<Integer> intCallable = () -> 42;
        Future<Integer> intFuture = mock(Future.class);
        when(executorService.submit(intCallable)).thenReturn(intFuture);

        Future<Integer> intResult = ClusterTrainingExecutor.getInstance().submit(intCallable);

        assertEquals(intFuture, intResult);
        verify(executorService, times(1)).submit(intCallable);
    }

    public void testThreadPoolName_hasCorrectValue() {
        assertEquals("cluster_training_thread_pool", SparseConstants.THREAD_POOL_NAME);
    }

    public void testMultipleRuns_callsExecutorMultipleTimes() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        Runnable runnable1 = mock(Runnable.class);
        Runnable runnable2 = mock(Runnable.class);

        ClusterTrainingExecutor.getInstance().run(runnable1);
        ClusterTrainingExecutor.getInstance().run(runnable2);

        verify(threadPool, times(2)).executor(SparseConstants.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(runnable1);
        verify(executorService, times(1)).execute(runnable2);
    }

    public void testSubmit_withNullCallable_passesToExecutor() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        ClusterTrainingExecutor.getInstance().submit(null);

        verify(threadPool, times(1)).executor(SparseConstants.THREAD_POOL_NAME);
        verify(executorService, times(1)).submit((Callable<Object>) null);
    }

    public void testRun_withNullRunnable_passesToExecutor() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        ClusterTrainingExecutor.getInstance().run(null);
        verify(threadPool, times(1)).executor(SparseConstants.THREAD_POOL_NAME);
        verify(executorService, times(1)).execute(null);
    }

    public void testUpdateThreadPoolSize_updatesThreadPoolWithCorrectSettings() {
        ClusterTrainingExecutor.getInstance().initialize(threadPool);
        Integer newThreadQty = 10;

        ArgumentCaptor<Settings> settingsCaptor = ArgumentCaptor.forClass(Settings.class);

        ClusterTrainingExecutor.updateThreadPoolSize(newThreadQty);

        verify(threadPool).setThreadPool(settingsCaptor.capture());

        Settings capturedSettings = settingsCaptor.getValue();

        String expectedKey = SparseConstants.THREAD_POOL_NAME + ".size";
        assertTrue("Settings should contain the thread pool size key", capturedSettings.keySet().contains(expectedKey));
    }
}
