/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import lombok.Getter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

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
        registerSettingsCallbacks(clusterService);
    }

    private void registerSettingsCallbacks(ClusterService clusterService) {
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
    }
}
