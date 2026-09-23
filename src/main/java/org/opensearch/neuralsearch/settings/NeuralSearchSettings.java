/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import org.opensearch.common.settings.Setting;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;

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
     * @deprecated
     * Setting representing how many documents are stored per group per subquery in HybridCollapsingTopDocsCollector
     * Default is set to 0, which will use the size passed via the query instead of 0, which is the standard practice for non-collapse hybrid search.
     *
     */
    @Deprecated
    public static final Setting<Integer> HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY = Setting.intSetting(
        "index.neural_search.hybrid_collapse_docs_per_group_per_subquery",
        0,
        0,
        1000,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
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

    /**
     * Kept in step with the per-query leg limit rather than picked: one {@code hybrid} may declare
     * {@link HybridQueryBuilder#MAX_NUMBER_OF_SUB_QUERIES} legs, so the square is what a request nesting fused hybrids
     * that deep costs, and no shape a single {@code hybrid} can express is affected by this ceiling.
     */
    public static final int DEFAULT_MAX_FUSION_LEG_SEARCHES = HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES
        * HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

    /**
     * The most {@link #MAX_FUSION_LEG_SEARCHES} can be raised to — the cube of the per-query leg limit, i.e. one more level
     * of fully-nested fused hybrids than the default admits. Derived the same way as the default rather than picked, and
     * present because a ceiling on the fan-out that could itself be raised without bound would not be a ceiling: the
     * request-level leg budget is the fail-closed backstop the rest of fused mode's resource guards lean on, so the setting
     * that carries it needs a maximum as well as a minimum.
     */
    public static final int MAX_FUSION_LEG_SEARCHES_CEILING = DEFAULT_MAX_FUSION_LEG_SEARCHES
        * HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

    /**
     * Ceiling on the leg sub-searches one search request may fan out in the {@code hybrid} query's fused mode — the sum of
     * {@code queries} sizes over every fused {@code hybrid} in the request body, whether nested or side by side. Each such
     * leg is a full search across the request's shards, so this is the request's fan-out multiplier and the reason the
     * limit is expressed in the same units the user wrote.
     *
     * <p>Counterpart to {@code indices.query.bool.max_clause_count}, and deliberately shaped like it: a whole-request count
     * of a declared unit, checked once, adjustable per cluster within fixed bounds. The floor is
     * {@link HybridQueryBuilder#MAX_NUMBER_OF_SUB_QUERIES} — lowering it below the number of legs a single {@code hybrid}
     * is already allowed to declare would reject a plain, un-nested fused query, so that is not an available setting. The
     * maximum is {@link #MAX_FUSION_LEG_SEARCHES_CEILING}: operator headroom of a few times the default, but never a value
     * at which the budget stops bounding anything.
     */
    public static final Setting<Integer> MAX_FUSION_LEG_SEARCHES = Setting.intSetting(
        "plugins.neural_search.hybrid.fusion.max_leg_searches",
        DEFAULT_MAX_FUSION_LEG_SEARCHES,
        HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES,
        MAX_FUSION_LEG_SEARCHES_CEILING,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Opt-in for the {@code hybrid} query's fused mode (in-query fusion), which is off until an operator turns it on.
     *
     * <p>Off refuses a query carrying a {@code fusion} block with a validation error rather than downgrading it to
     * classic hybrid: classic hybrid normalizes in a search pipeline, so a request whose whole fusion config is written
     * in the query body has none, and a silent downgrade would answer 200 with un-normalized scores and a different
     * ranking. Dynamic, so the same setting turns fused mode on and — if it has to be — off again without a restart.
     *
     * <p>Both properties are load-bearing. {@code NodeScope} is the {@code opensearch.yml} route: declarative, at home in
     * whatever configures the nodes, carried by a replacement node without an API call, and the only way to bring a
     * cluster up with fused mode already on. {@code Dynamic} is the {@code PUT /_cluster/settings} route: it needs no
     * restart, and it is the only route on a cluster whose node configuration cannot be edited in place. Neither property
     * is redundant — dropping either one takes the feature away from a way of running OpenSearch.
     *
     * <p>The two routes are not equal when both are used. A cluster-state value wins over the node's own
     * {@code opensearch.yml} — {@code AbstractScopedSettings} resolves the applied cluster settings first and falls back
     * to the node's settings — so once the key has been set through the API the yml line no longer decides anything.
     *
     * <p>Set it under {@code persistent}, not {@code transient}: a transient value silently overrides the persistent one
     * while it exists and is then lost on a full-cluster restart, which would turn fused mode off under a workload already
     * relying on it.
     */
    public static final Setting<Boolean> HYBRID_FUSION_ENABLED = Setting.boolSetting(
        "plugins.neural_search.hybrid.fusion.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How much the fused fast path may fetch beyond the page round 2 would have fetched — {@code (legs × window − size)}
     * documents, weighed by the {@code _source} size observed for the index and the embedding payload the mapping
     * declares for requested fields — before it is refused and the request runs the usual two rounds.
     *
     * <p>The default is set for a tiered coordinator/worker topology, where 1.3–3 MB of extra documents cost as much as
     * the round saved. On a co-located cluster the crossover is higher (measured above 3.7 MB on 3 × r5.xlarge), so an
     * operator there can raise it; {@code 0} refuses the fast path for any request that fetches anything. Dynamic and
     * read per request. A latency knob only: both paths return the same page.
     */
    public static final Setting<ByteSizeValue> HYBRID_FUSION_FAST_PATH_FETCH_BUDGET = Setting.byteSizeSetting(
        "plugins.neural_search.hybrid.fusion.fast_path_fetch_budget",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        ByteSizeValue.ZERO,
        new ByteSizeValue(1, ByteSizeUnit.GB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
