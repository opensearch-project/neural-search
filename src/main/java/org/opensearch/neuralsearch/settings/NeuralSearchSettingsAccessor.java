/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import lombok.Getter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.cache.CircuitBreakerManager;
import org.opensearch.neuralsearch.sparse.cache.MemoryUsageManager;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_OVERHEAD;

/**
 * Class handles exposing settings related to neural search and manages callbacks when the settings change
 */
public class NeuralSearchSettingsAccessor {
    @Getter
    private volatile boolean isStatsEnabled;

    @Getter
    private volatile boolean isAgenticSearchEnabled;

    /**
     * Constructor, registers callbacks to update settings
     * @param clusterService
     * @param settings
     */
    public NeuralSearchSettingsAccessor(ClusterService clusterService, Settings settings) {
        isStatsEnabled = NeuralSearchSettings.NEURAL_STATS_ENABLED.get(settings);
        isAgenticSearchEnabled = NeuralSearchSettings.AGENTIC_SEARCH_ENABLED.get(settings);
        registerSettingsCallbacks(clusterService, settings);
    }

    private void registerSettingsCallbacks(ClusterService clusterService, Settings settings) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NeuralSearchSettings.NEURAL_STATS_ENABLED, value -> {
            // If stats are being toggled off, clear and reset all stats
            if (isStatsEnabled && (value == false)) {
                EventStatsManager.instance().reset();
            }
            isStatsEnabled = value;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NeuralSearchSettings.AGENTIC_SEARCH_ENABLED, value -> {
            isAgenticSearchEnabled = value;
        });
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NEURAL_CIRCUIT_BREAKER_LIMIT, NEURAL_CIRCUIT_BREAKER_OVERHEAD, (limit, overhead) -> {
                CircuitBreakerManager.setLimitAndOverhead(limit, overhead);
                MemoryUsageManager.getInstance().setLimitAndOverhead(limit, overhead);
            });
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING, (setting) -> {
                int maxThreadQty = OpenSearchExecutors.allocatedProcessors(settings);
                ClusterTrainingExecutor.updateThreadPoolSize(maxThreadQty, setting);
            });
    }
}
