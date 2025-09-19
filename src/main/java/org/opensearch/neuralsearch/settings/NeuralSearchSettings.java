/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import java.util.List;

import org.opensearch.common.settings.Setting;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Class defines settings specific to neural-search plugin
 * DEFAULT_INDEX_THREAD_QTY: -1 represents that user did not give a specific thread quantity
 * MAX_INDEX_THREAD_QTY: Initial max value, will be updated based on actual CPU cores
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NeuralSearchSettings {

    public static final String SPARSE_ALGO_PARAM_INDEX_THREAD_QTY = "plugins.neural_search.sparse.algo_param.index_thread_qty";
    public static final String NEURAL_CIRCUIT_BREAKER_NAME = "neural_search";
    public static final int DEFAULT_INDEX_THREAD_QTY = 1; // Choosing 1 as default value to protect safety
    public static final int MINIMUM_INDEX_THREAD_QTY = 1;
    public static final int MAXIMUM_INDEX_THREAD_QTY = 1024;

    // Constants for system-generated factories setting
    public static final String SYSTEM_GENERATED_FACTORIES_KEY = "cluster.search.enabled_system_generated_factories";
    public static final String SEMANTIC_HIGHLIGHTER_FACTORY_NAME = "semantic-highlighter";
    public static final List<String> DEFAULT_ENABLED_SYSTEM_FACTORIES = List.of(SEMANTIC_HIGHLIGHTER_FACTORY_NAME);

    /**
     * Specifies the initial memory limit for the parent circuit breaker.
     * Defaults to 10% of the JVM heap.
     */
    private static final String DEFAULT_CIRCUIT_BREAKER_LIMIT = "10%";
    /**
     * A constant by which the neural data estimations are multiplied to determine the final estimation.
     * Default is 1.0 while minimum is 0.0.
     */
    private static final double DEFAULT_CIRCUIT_BREAKER_OVERHEAD = 1.0d;
    private static final double MINIMUM_CIRCUIT_BREAKER_OVERHEAD = 0.0d;

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
     * Setting representing how many documents are stored per group per subquery in HybridCollapsingTopDocsCollector
     * Default is set to 0, which will use the size passed via the query instead of 0, which is the standard practice for non-collapse hybrid search.
     *
     */
    public static final Setting<Integer> HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY = Setting.intSetting(
        "index.neural_search.hybrid_collapse_docs_per_group_per_subquery",
        0,
        0,
        1000,
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

    public static Setting<Integer> SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
        DEFAULT_INDEX_THREAD_QTY,
        MINIMUM_INDEX_THREAD_QTY,
        MAXIMUM_INDEX_THREAD_QTY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * A constant by which the neural memory estimations are multiplied to determine the final estimation. Default is 1.
     */
    public static final Setting<Double> NEURAL_CIRCUIT_BREAKER_OVERHEAD = Setting.doubleSetting(
        "plugins.neural_search.circuit_breaker.overhead",
        DEFAULT_CIRCUIT_BREAKER_OVERHEAD,
        MINIMUM_CIRCUIT_BREAKER_OVERHEAD,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The memory limit for neural circuit breaker. Default is 10% of the JVM heap.
     */
    public static final Setting<ByteSizeValue> NEURAL_CIRCUIT_BREAKER_LIMIT = Setting.memorySizeSetting(
        "plugins.neural_search.circuit_breaker.limit",
        DEFAULT_CIRCUIT_BREAKER_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
