/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.opensearch.neuralsearch.sparse.common.SparseConstants;

/**
 * Singleton thread pool manager for cluster training operations.
 */
public class ClusterTrainingExecutor {
    private ThreadPool threadpool = null;
    private static ClusterTrainingExecutor INSTANCE;

    /**
     * Initializes the thread pool.
     *
     * @param threadPool the OpenSearch thread pool
     */
    public void initialize(ThreadPool threadPool) {
        this.threadpool = threadPool;
    }

    /**
     * Returns the singleton instance.
     *
     * @return the singleton instance
     */
    public static ClusterTrainingExecutor getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ClusterTrainingExecutor();
        }
        return INSTANCE;
    }

    /**
     * Gets the executor.
     *
     * @return the cluster training executor
     */
    public Executor getExecutor() {
        return threadpool.executor(SparseConstants.THREAD_POOL_NAME);
    }

    /**
     * Executes a task asynchronously.
     *
     * @param runnable the task to execute
     */
    public void run(Runnable runnable) {
        threadpool.executor(SparseConstants.THREAD_POOL_NAME).execute(runnable);
    }

    /**
     * Submits a callable task.
     *
     * @param <T> the return type
     * @param callable the task to submit
     * @return a Future representing the result
     */
    public <T> Future<T> submit(Callable<T> callable) {
        return threadpool.executor(SparseConstants.THREAD_POOL_NAME).submit(callable);
    }

    /**
     * Updates the thread pool size.
     *
     * @param newThreadQty the new thread count
     */
    public static void updateThreadPoolSize(Integer newThreadQty) {
        Settings threadPoolSettings = Settings.builder()
            .put(String.format(Locale.ROOT, "%s.size", SparseConstants.THREAD_POOL_NAME), newThreadQty)
            .build();
        INSTANCE.threadpool.setThreadPool(threadPoolSettings);
    }
}
