/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.lucene.search.TaskExecutor;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HybridQueryExecutor {
    private static final String HYBRID_QUERY_EXEC_THREAD_POOL_NAME = "_plugin_neural_search_hybrid_query_executor";
    private static final Integer HYBRID_QUERY_EXEC_THREAD_POOL_QUEUE_SIZE = 1000;
    private static TaskExecutor taskExecutor;

    /**
     * Provide fixed executor builder to use for hybrid query executors
     * @param settings Node level settings
     * @return the executor builder for hybrid query's custom thread pool.
     */
    public static ExecutorBuilder getExecutorBuilder(final Settings settings) {
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        return new FixedExecutorBuilder(
            settings,
            HYBRID_QUERY_EXEC_THREAD_POOL_NAME,
            allocatedProcessors,
            HYBRID_QUERY_EXEC_THREAD_POOL_QUEUE_SIZE,
            HYBRID_QUERY_EXEC_THREAD_POOL_NAME
        );
    }

    /**
     * Initialize @{@link TaskExecutor} to run tasks concurrently using {@link ThreadPool}
     * @param threadPool OpenSearch's thread pool instance
     */
    public static void initialize(@NonNull ThreadPool threadPool) {
        if (threadPool == null) {
            throw new IllegalArgumentException("Argument thread-pool cannot be null");
        }
        taskExecutor = new TaskExecutor(threadPool.executor(HYBRID_QUERY_EXEC_THREAD_POOL_NAME));
    }

    /**
     * Return TaskExecutor Wrapper that helps runs tasks concurrently
     * @return TaskExecutor instance to help run search tasks in parallel
     */
    public static TaskExecutor getExecutor() {
        return taskExecutor != null ? taskExecutor : new TaskExecutor(Runnable::run);
    }

    @VisibleForTesting
    public static String getThreadPoolName() {
        return HYBRID_QUERY_EXEC_THREAD_POOL_NAME;
    }
}
