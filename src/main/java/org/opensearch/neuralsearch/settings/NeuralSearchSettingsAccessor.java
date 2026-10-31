/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.cache.CircuitBreakerManager;
import org.opensearch.neuralsearch.sparse.cache.MemoryUsageManager;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_FUSION_ENABLED;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_OVERHEAD;

/**
 * Class handles exposing settings related to neural search and manages callbacks when the settings change
 */
@Log4j2
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
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NEURAL_CIRCUIT_BREAKER_LIMIT, NEURAL_CIRCUIT_BREAKER_OVERHEAD, (limit, overhead) -> {
                CircuitBreakerManager.setLimitAndOverhead(limit, overhead);
                MemoryUsageManager.getInstance().setLimitAndOverhead(limit, overhead);
            });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(HYBRID_FUSION_ENABLED, value -> {
            // Nothing to cache: the gate reads this setting live, per request, so a request needs no state from here. The
            // consumer exists to put the transition in the node log, because turning fused mode off (or back on) changes
            // the answer to any request whose body carries a fusion block, and a cluster setting leaves no other trace of
            // when that happened. WARN, not INFO, because it is a relevance change, and only a real change reaches a
            // consumer, so one line means one flip.
            log.warn(
                "[{}] is now [{}] - a query carrying a fusion block will be {} on this cluster",
                HYBRID_FUSION_ENABLED.getKey(),
                value,
                value ? "accepted" : "refused"
            );
        });
    }
}
