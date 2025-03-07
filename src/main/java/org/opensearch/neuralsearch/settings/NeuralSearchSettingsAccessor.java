/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import lombok.Getter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

public class NeuralSearchSettingsAccessor {
    private static NeuralSearchSettingsAccessor INSTANCE;
    private boolean initialized;

    @Getter
    private volatile Boolean isStatsEnabled;

    /**
     * Return instance of the cluster context, must be initialized first for proper usage
     * @return instance of cluster context
     */
    public static synchronized NeuralSearchSettingsAccessor instance() {
        if (INSTANCE == null) {
            INSTANCE = new NeuralSearchSettingsAccessor();
        }
        return INSTANCE;
    }

    public void initialize(ClusterService clusterService, Settings settings) {
        if (initialized) return;

        isStatsEnabled = NeuralSearchSettings.NEURAL_STATS_ENABLED.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(NeuralSearchSettings.NEURAL_STATS_ENABLED, value -> {
            // If stats are being toggled off, clear and reset all stats
            if (isStatsEnabled && (value == false)) {
                EventStatsManager.instance().reset();
            }
            isStatsEnabled = value;
        });
    }

}
