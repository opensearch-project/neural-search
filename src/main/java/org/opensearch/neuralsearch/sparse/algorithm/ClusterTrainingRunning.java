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

/**
 * Thread pool manager for cluster training operations in sparse neural search.
 *
 * <p>This singleton class manages a dedicated thread pool for executing
 * clustering training tasks asynchronously. It provides thread-safe access
 * to the thread pool and allows dynamic configuration of thread pool size.
 *
 * <p>The class is designed to handle computationally intensive clustering
 * operations without blocking the main search threads, ensuring optimal
 * performance during neural search operations.
 *
 * @see ThreadPool
 */
public class ClusterTrainingRunning {
    private static ThreadPool threadpool = null;
    private static ClusterTrainingRunning INSTANCE;
    public static final String THREAD_POOL_NAME = "cluster_training_thread_pool";

    /**
     * Initializes the thread pool for cluster training operations.
     *
     * @param threadPool the OpenSearch thread pool to use for cluster training
     */
    public static void initialize(ThreadPool threadPool) {
        ClusterTrainingRunning.threadpool = threadPool;
    }

    /**
     * Returns the singleton instance of ClusterTrainingRunning.
     *
     * @return the singleton instance, creating it if necessary
     */
    public static synchronized ClusterTrainingRunning getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ClusterTrainingRunning();
        }
        return INSTANCE;
    }

    /**
     * Gets the executor for cluster training tasks.
     *
     * @return the executor for the cluster training thread pool
     */
    public Executor getExecutor() {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME);
    }

    /**
     * Executes a runnable task asynchronously on the cluster training thread pool.
     *
     * @param runnable the task to execute
     */
    public void run(Runnable runnable) {
        ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).execute(runnable);
    }

    /**
     * Submits a callable task for asynchronous execution.
     *
     * @param <T> the return type of the callable
     * @param callable the task to submit
     * @return a Future representing the pending result
     */
    public <T> Future<T> submit(Callable<T> callable) {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).submit(callable);
    }

    /**
     * Updates the thread pool size dynamically.
     *
     * @param newThreadQty the new number of threads for the pool
     */
    public static void updateThreadPoolSize(Integer newThreadQty) {
        Settings threadPoolSettings = Settings.builder()
            .put(String.format(Locale.ROOT, "%s.size", ClusterTrainingRunning.THREAD_POOL_NAME), newThreadQty)
            .build();
        threadpool.setThreadPool(threadPoolSettings);
    }
}
