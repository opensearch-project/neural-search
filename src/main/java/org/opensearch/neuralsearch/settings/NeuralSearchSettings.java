/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import org.opensearch.common.settings.Setting;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Class defines settings specific to neural-search plugin
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NeuralSearchSettings {
    /**
     * Gates the functionality of hybrid search
     * Currently query phase searcher added with hybrid search will conflict with concurrent search in core.
     * Once that problem is resolved this feature flag can be removed.
     */
    public static final Setting<Boolean> NEURAL_SEARCH_HYBRID_SEARCH_DISABLED = Setting.boolSetting(
        "plugins.neural_search.hybrid_search_disabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Limits the number of document fields that can be passed to the reranker.
     */
    public static final Setting<Integer> RERANKER_MAX_DOC_FIELDS = Setting.intSetting(
        "plugins.neural_search.reranker_max_document_fields",
        50,
        Setting.Property.NodeScope
    );

    /**
     * Use this setting to manage if a neuralSparseQuery build a two-phase query of not.
     */
    public static final Setting<Boolean> NEURAL_SPARSE_TWO_PHASE_DEFAULT_ENABLED = Setting.boolSetting(
        "index.neural_sparse.two_phase.default_enabled",
        true,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Control the number of TopDocs rescored by the second phase of NeuralSparseQuery.
     */
    public static final Setting<Float> NEURAL_SPARSE_TWO_PHASE_DEFAULT_WINDOW_SIZE_EXPANSION = Setting.floatSetting(
        "index.neural_sparse.two_phase.default_window_size_expansion",
        5f,
        1f,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Control the token score threshold to splitting the NeuralSparseQuery.
     */
    public static final Setting<Float> NEURAL_SPARSE_TWO_PHASE_DEFAULT_PRUNING_RATIO = Setting.floatSetting(
        "index.neural_sparse.two_phase.default_pruning_ratio",
        0.4f,
        0.0f,
        1.0f,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Control the max rescore windows size of the second phase of NeuralSparseQuery.
     * The default value is consistent with core settings MAX_RESCORE_WINDOW_SETTING.
     */
    public static final Setting<Integer> NEURAL_SPARSE_TWO_PHASE_MAX_WINDOW_SIZE = Setting.intSetting(
        "index.neural_sparse.two_phase.max_window_size",
        10000,
        50,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
}
