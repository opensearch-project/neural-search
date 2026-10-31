/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_FUSION_ENABLED;

import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchSettingsAccessorTests extends OpenSearchTestCase {

    private static final String LOGGER_NAME = NeuralSearchSettingsAccessor.class.getCanonicalName();

    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                NeuralSearchSettings.NEURAL_STATS_ENABLED,
                NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT,
                NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_OVERHEAD,
                SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
                HYBRID_FUSION_ENABLED
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        // Registering the callbacks is the whole point of the constructor; it also asserts the setting is registered, since
        // addSettingsUpdateConsumer rejects a key the cluster settings do not know.
        new NeuralSearchSettingsAccessor(clusterService, Settings.EMPTY);
    }

    public void testFusedModeSwitch_whenTurnedOff_thenTransitionIsLogged() throws IllegalAccessException {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "fused mode turned off",
                    LOGGER_NAME,
                    Level.WARN,
                    "[" + HYBRID_FUSION_ENABLED.getKey() + "] is now [false]*refused*"
                )
            );
            clusterSettings.applySettings(Settings.builder().put(HYBRID_FUSION_ENABLED.getKey(), false).build());
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFusedModeSwitch_whenTurnedBackOn_thenTransitionIsLogged() throws IllegalAccessException {
        clusterSettings.applySettings(Settings.builder().put(HYBRID_FUSION_ENABLED.getKey(), false).build());
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "fused mode back on its default",
                    LOGGER_NAME,
                    Level.WARN,
                    "[" + HYBRID_FUSION_ENABLED.getKey() + "] is now [true]*accepted*"
                )
            );
            // Clearing the key returns the switch to its default, which is on — the same transition as an explicit true.
            clusterSettings.applySettings(Settings.EMPTY);
            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * The negative control the two positives need: an update that leaves fused mode alone must not log about it, otherwise
     * every unrelated cluster settings change would look like a flip of the switch.
     */
    public void testFusedModeSwitch_whenAnotherSettingChanges_thenNothingIsLogged() throws IllegalAccessException {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "fused mode untouched",
                    LOGGER_NAME,
                    Level.WARN,
                    "*" + HYBRID_FUSION_ENABLED.getKey() + "*"
                )
            );
            clusterSettings.applySettings(Settings.builder().put(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), true).build());
            appender.assertAllExpectationsMatched();
        }
    }
}
