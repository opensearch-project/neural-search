/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.After;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the two-gate native engine flag. The gates are AND-ed: the static
 * {@link SparseSettings#SPARSE_NATIVE_ENGINE_FEATURE_ENABLED} from opensearch.yml (default true) and
 * the dynamic {@link SparseSettings#SPARSE_NATIVE_ENGINE_ENABLED} (default false).
 */
public class SparseSettingsTests extends AbstractSparseTestBase {

    @After
    @Override
    public void tearDown() throws Exception {
        SparseSettings.reset();
        super.tearDown();
    }

    /** Initializes the singleton as a node started with these two flag values would be. */
    private void initializeWith(boolean featureEnabled, boolean dynamicEnabled) {
        Settings nodeSettings = Settings.builder()
            .put(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED, featureEnabled)
            .put(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, dynamicEnabled)
            .build();
        // Every node-scoped sparse setting: IS_SPARSE_INDEX_SETTING is index-scoped and cannot go
        // into ClusterSettings, the rest are what a real node registers.
        ClusterSettings clusterSettings = new ClusterSettings(
            nodeSettings,
            Set.of(
                SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING,
                SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING,
                SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        // initialize() also sizes the clustering thread pool, which needs its own singleton up
        ClusterTrainingExecutor.getInstance().initialize(mock(ThreadPool.class));
        SparseSettings.reset();
        SparseSettings.state().initialize(clusterService, nodeSettings);
    }

    public void testDefaults() {
        assertTrue(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING.getDefault(Settings.EMPTY));
        assertFalse(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING.getDefault(Settings.EMPTY));
    }

    public void testFeatureFlagIsStaticAndDynamicSettingIsDynamic() {
        assertFalse(
            "the opensearch.yml flag must not be updatable at runtime",
            SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING.isDynamic()
        );
        assertTrue(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING.hasNodeScope());

        assertTrue(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING.isDynamic());
        assertTrue(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING.hasNodeScope());
    }

    public void testBothSettingsAreRegistered() {
        var registered = SparseSettings.state().getSettings();
        assertTrue(registered.contains(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING));
        assertTrue(registered.contains(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING));
    }

    public void testEnabledOnlyWhenBothGatesAreOpen() {
        initializeWith(true, true);
        assertTrue(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testDisabledWhenOnlyTheFeatureFlagIsOn() {
        // The shipped default: opensearch.yml permits it but no operator turned it on.
        initializeWith(true, false);
        assertFalse(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testDisabledWhenOnlyTheDynamicSettingIsOn() {
        initializeWith(false, true);
        assertFalse(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testDisabledWhenBothGatesAreClosed() {
        initializeWith(false, false);
        assertFalse(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testFeatureFlagOffShortCircuitsBeforeReadingClusterState() {
        // A false static flag must not need a cluster service at all, so a node that never
        // initialized cannot NPE its way past the gate.
        Settings nodeSettings = Settings.builder().put(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED, false).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(nodeSettings, Set.of(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING))
        );

        ClusterTrainingExecutor.getInstance().initialize(mock(ThreadPool.class));
        SparseSettings.reset();
        SparseSettings.state().initialize(clusterService, nodeSettings);

        assertFalse(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testDisabledBeforeInitialize() {
        SparseSettings.reset();
        assertFalse("an uninitialized node must report the engine as off, not throw", SparseSettings.state().isNativeEngineEnabled());
    }

    public void testDynamicSettingIsReadLiveNotCached() {
        initializeWith(true, false);
        assertFalse(SparseSettings.state().isNativeEngineEnabled());

        // Same instance, updated cluster state: the gate has to follow it without a restart.
        Settings updated = Settings.builder().put(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED, true).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING, SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING)
        );
        clusterSettings.applySettings(updated);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ClusterTrainingExecutor.getInstance().initialize(mock(ThreadPool.class));
        SparseSettings.state().initialize(clusterService, Settings.EMPTY);

        assertTrue(SparseSettings.state().isNativeEngineEnabled());
    }

    public void testGetSettingValueRejectsUnknownKey() {
        initializeWith(true, true);
        expectThrows(IllegalArgumentException.class, () -> SparseSettings.state().getSettingValue("plugins.neural_search.sparse.nope"));
    }

    public void testGetSettingValueFallsBackToDefaultsBeforeInitialize() {
        SparseSettings.reset();

        // The codec reads this during a flush, so an uninitialized node must not throw
        assertEquals(
            SparseSettings.DEFAULT_INDEX_THREAD_QTY,
            (int) SparseSettings.state().getSettingValue(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY)
        );
    }

    public void testGetSettingsListsEverySparseSetting() {
        // An unlisted setting is unregistered at node start, and reading one then throws rather than
        // falling back to its default -- so the list, not the field count, is what has to be complete.
        assertEquals(4, SparseSettings.state().getSettings().size());
        assertTrue(SparseSettings.state().getSettings().contains(SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING));
        assertTrue(SparseSettings.state().getSettings().contains(SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING));
    }
}
