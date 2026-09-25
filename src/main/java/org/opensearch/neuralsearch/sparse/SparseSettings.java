/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.IndexModule;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;

import java.util.List;

import static org.opensearch.common.settings.Setting.Property.Final;
import static org.opensearch.common.settings.Setting.Property.IndexScope;
import static org.opensearch.common.settings.Setting.Property.UnmodifiableOnRestore;

/**
 * It holds index settings of a sparse vector index.
 */
public class SparseSettings {
    public static final String SPARSE_INDEX = "index.sparse";
    public static final String SPARSE_ALGO_PARAM_INDEX_THREAD_QTY = "plugins.neural_search.sparse.algo_param.index_thread_qty";
    public static final String SPARSE_NATIVE_ENGINE_FEATURE_ENABLED = "plugins.neural_search.sparse.native_engine_feature_enabled";
    public static final String SPARSE_NATIVE_ENGINE_ENABLED = "plugins.neural_search.sparse.native_engine_enabled";

    /** Shared reason string, so every gate rejects with the same wording. */
    public static final String NATIVE_ENGINE_DISABLED_REASON = "the native sparse engine is disabled: both ["
        + SPARSE_NATIVE_ENGINE_FEATURE_ENABLED
        + "] and ["
        + SPARSE_NATIVE_ENGINE_ENABLED
        + "] must be enabled";

    /**
     * The engine reads its index by mmapping the engine file itself, outside Lucene's
     * {@link org.apache.lucene.store.Directory}, and has no other read path -- so a node that
     * disallows mmap cannot serve the engine at all, and the whole feature has to be refused rather
     * than degraded. Core will not refuse it for us: its own check covers only the {@code mmapfs}
     * and {@code hybridfs} store types, and nothing stops a plugin from mapping. The same setting
     * also turns off the {@code vm.max_map_count} bootstrap check, so on such a node nobody has
     * verified there is room to map anything.
     */
    public static final String MMAP_DISALLOWED_REASON = "the native sparse engine requires mmap, which this node disallows via ["
        + IndexModule.NODE_STORE_ALLOW_MMAP.getKey()
        + "]";

    public static final int DEFAULT_INDEX_THREAD_QTY = 1; // Choosing 1 as default value to protect safety
    public static final int MINIMUM_INDEX_THREAD_QTY = 1;
    public static final int MAXIMUM_INDEX_THREAD_QTY = 1024;

    private static SparseSettings INSTANCE;
    private ClusterService clusterService;
    // Only the static gate; the dynamic one is read live in isNativeEngineEnabled(). Read once off
    // the node settings: the static flag has no cluster state to consult, and a node that never
    // called initialize() must still answer.
    private boolean staticNativeEngineEnabled = SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING.getDefault(Settings.EMPTY);
    // Node-scoped and static like the flag above, and owned by core rather than by us, so it is read
    // off the node settings the same way rather than from cluster state.
    private boolean mmapAllowed = IndexModule.NODE_STORE_ALLOW_MMAP.getDefault(Settings.EMPTY);

    public static synchronized SparseSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new SparseSettings();
        }
        return INSTANCE;
    }

    /**
     * Drops the singleton so the next {@link #state()} starts from the setting defaults. The
     * instance is process-wide mutable state; without this a test that initializes it leaks into
     * every test that runs after it in the same JVM.
     */
    @VisibleForTesting
    public static synchronized void reset() {
        INSTANCE = null;
    }

    public void initialize(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        this.staticNativeEngineEnabled = SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING.get(settings);
        this.mmapAllowed = IndexModule.NODE_STORE_ALLOW_MMAP.get(settings);
        registerSettingsCallbacks(clusterService, settings);
    }

    /**
     * The native sparse engine is on only when both gates are open: the static
     * {@link #SPARSE_NATIVE_ENGINE_FEATURE_ENABLED} flag from opensearch.yml (default true) and
     * the dynamic {@link #SPARSE_NATIVE_ENGINE_ENABLED} setting (default false). Either one being
     * off disables it, as does a node that disallows mmap.
     */
    public boolean isNativeEngineEnabled() {
        return nativeEngineDisabledReason() == null;
    }

    /**
     * Why this node cannot use the native sparse engine, or null when it can. Every gate rejects
     * with the reason this returns, so an operator is told which of the three settings to change
     * rather than that the engine is off.
     */
    public String nativeEngineDisabledReason() {
        if (staticNativeEngineEnabled == false) {
            return NATIVE_ENGINE_DISABLED_REASON;
        }
        // Before initialize() this reads the dynamic gate's default, which is off -- the engine is
        // not assumed available on a node that has no cluster state yet.
        if (Boolean.TRUE.equals(getSettingValue(SPARSE_NATIVE_ENGINE_ENABLED)) == false) {
            return NATIVE_ENGINE_DISABLED_REASON;
        }
        if (mmapAllowed == false) {
            return MMAP_DISALLOWED_REASON;
        }
        return null;
    }

    private void registerSettingsCallbacks(ClusterService clusterService, Settings settings) {
        // Initialize with current values since addSettingsUpdateConsumer only fires on updates
        int initialThreadQty = SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(clusterService.getSettings());
        int maxThreadQty = OpenSearchExecutors.allocatedProcessors(settings);
        ClusterTrainingExecutor.updateThreadPoolSize(maxThreadQty, initialThreadQty);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING, (setting) -> {
                int maxThreads = OpenSearchExecutors.allocatedProcessors(settings);
                ClusterTrainingExecutor.updateThreadPoolSize(maxThreads, setting);
            });
    }

    /**
     * Current value of a sparse setting, or its default on a node that never called
     * {@link #initialize}. There is no cluster state to read before then, and every caller wants the
     * default rather than an NPE -- the codec reaches this during a flush, which must not fail.
     */
    public <T> T getSettingValue(String key) {
        Setting<?> setting = getSetting(key);
        if (clusterService == null) {
            return (T) setting.getDefault(Settings.EMPTY);
        }
        return (T) clusterService.getClusterSettings().get(setting);
    }

    public List<Setting<?>> getSettings() {
        return List.of(
            SparseSettings.IS_SPARSE_INDEX_SETTING,
            SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
            SparseSettings.SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING,
            SparseSettings.SPARSE_NATIVE_ENGINE_ENABLED_SETTING
        );
    }

    private Setting<?> getSetting(String key) {
        if (SPARSE_ALGO_PARAM_INDEX_THREAD_QTY.equals(key)) {
            return SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING;
        } else if (SPARSE_NATIVE_ENGINE_FEATURE_ENABLED.equals(key)) {
            return SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING;
        } else if (SPARSE_NATIVE_ENGINE_ENABLED.equals(key)) {
            return SPARSE_NATIVE_ENGINE_ENABLED_SETTING;
        }

        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
    }

    /**
     * This setting identifies sparse index.
     */
    public static final Setting<Boolean> IS_SPARSE_INDEX_SETTING = Setting.boolSetting(
        SPARSE_INDEX,
        false,
        IndexScope,
        Final,
        UnmodifiableOnRestore
    );

    /**
     * Static gate for the native sparse engine, set in opensearch.yml and read at node start.
     * Defaults to true, so a deployment opts out rather than in; pair it with
     * {@link #SPARSE_NATIVE_ENGINE_ENABLED_SETTING}, which an operator must turn on.
     */
    public static final Setting<Boolean> SPARSE_NATIVE_ENGINE_FEATURE_ENABLED_SETTING = Setting.boolSetting(
        SPARSE_NATIVE_ENGINE_FEATURE_ENABLED,
        true,
        Setting.Property.NodeScope
    );

    /**
     * Dynamic gate for the native sparse engine. Defaults to false: the engine stays off until an
     * operator turns it on, and can be turned back off without a restart.
     */
    public static final Setting<Boolean> SPARSE_NATIVE_ENGINE_ENABLED_SETTING = Setting.boolSetting(
        SPARSE_NATIVE_ENGINE_ENABLED,
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static Setting<Integer> SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
        DEFAULT_INDEX_THREAD_QTY,
        MINIMUM_INDEX_THREAD_QTY,
        MAXIMUM_INDEX_THREAD_QTY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

}
