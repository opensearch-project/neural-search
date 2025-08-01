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
     * Limits the number of document fields that can be passed to the reranker.
     */
    public static final Setting<Integer> RERANKER_MAX_DOC_FIELDS = Setting.intSetting(
        "plugins.neural_search.reranker_max_document_fields",
        50,
        Setting.Property.NodeScope
    );

    /**
     * Enables or disables the Stats API and event stat collection.
     * If API is called when stats are disabled, the response will 403.
     * Event stat increment calls are also treated as no-ops.
     */
    public static final Setting<Boolean> NEURAL_STATS_ENABLED = Setting.boolSetting(
        "plugins.neural_search.stats_enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Configure the maximum number of docs we can batch ingest for the semantic field.
     */
    public static final Setting<Integer> SEMANTIC_INGEST_BATCH_SIZE = Setting.intSetting(
        "index.neural_search.semantic_ingest_batch_size",
        10,
        1,
        100,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Enables or disables agentic query clause
     */
    public static final Setting<Boolean> AGENTIC_SEARCH_ENABLED = Setting.boolSetting(
        "plugins.neural_search.agentic_search_enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
