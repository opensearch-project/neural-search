/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public class ClusterTrainingRunning {
    private static ThreadPool threadpool = null;
    private static ClusterTrainingRunning INSTANCE;
    public static final String THREAD_POOL_NAME = "cluster_training_thread_pool";

    public static void initialize(ThreadPool threadPool) {
        ClusterTrainingRunning.threadpool = threadPool;
    }

    public static synchronized ClusterTrainingRunning getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ClusterTrainingRunning();
        }
        return INSTANCE;
    }

    public Executor getExecutor() {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME);
    }

    public void run(Runnable runnable) {
        ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).execute(runnable);
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).submit(callable);
    }
}
