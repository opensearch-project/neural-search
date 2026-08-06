/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.QueryBuilderVisitor;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isVersionOnOrAfterMinReqVersionForFusedModeInHybridQuery;

/**
 * Class abstract creation of a Query type "hybrid". Hybrid query will allow execution of multiple sub-queries and
 * collects score for each of those sub-query.
 */
@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
public final class HybridQueryBuilder extends AbstractQueryBuilder<HybridQueryBuilder> {
    public static final String NAME = "hybrid";

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ParseField PAGINATION_DEPTH_FIELD = new ParseField("pagination_depth");
    private static final ParseField FUSION_FIELD = new ParseField("fusion");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private Integer paginationDepth;

    /**
     * Resolver (fused) mode config: the raw inline {@code fusion} block from the query body. Its <b>presence</b> is the
     * resolver on/off flag; its <b>shape</b> carries the config. {@code null} = classic hybrid (byte-identical wire
     * form). A string {@code "pipeline"} is normalized to {@code {source: "pipeline"}} at parse. In this build the
     * parameter is parsed, validated, and round-tripped, but execution is not yet wired — a well-formed {@code fusion}
     * fails fast at {@link #doRewrite} (see PR sequencing in the LLD/tracker). Not serialized over the transport wire
     * yet (binary wire gate lands with the execution path).
     */
    private Map<String, Object> fusion;

    /**
     * Round-1 → round-2 bridge for the resolver (fused) mode. The async leg {@code MultiSearch} registered in
     * {@link #doRewriteFused} sets the self-erased standard query here; the next rewrite round returns it. {@code null}
     * on a freshly parsed builder (classic path never touches it).
     */
    private Supplier<QueryBuilder> fusedSupplier;

    public static final int MAX_NUMBER_OF_SUB_QUERIES = 5;
    private static final int LOWER_BOUND_OF_PAGINATION_DEPTH = 0;
    private static final int DEFAULT_FUSION_WINDOW_SIZE = 100;

    // Allowed top-level keys inside a `fusion` object; anything else is a parse-time 400.
    private static final String FUSION_KEY_SOURCE = "source";
    private static final String FUSION_KEY_NORMALIZATION = "normalization";
    private static final String FUSION_KEY_COMBINATION = "combination";
    private static final String FUSION_KEY_WINDOW_SIZE = "window_size";
    private static final String FUSION_SOURCE_PIPELINE = "pipeline";

    // Error message templates for reuse across REST and gRPC paths
    public static final String ERROR_MSG_QUERIES_REQUIRED = "[%s] requires 'queries' field with at least one clause";
    public static final String ERROR_MSG_MAX_QUERIES_EXCEEDED = "Number of sub-queries exceeds maximum supported by [%s] query";
    public static final String ERROR_MSG_BOOST_NOT_SUPPORTED = "[%s] query does not support [%s]";
    public static final String ERROR_MSG_FILTER_MUST_BE_QUERY_OBJECT = "[%s] query's [%s] field must be a query object";
    public static final String ERROR_MSG_FUSION_REACHED_SHARD =
        "[%s] query [%s] (resolver/fused mode) must self-erase at the coordinator and must not reach a shard";

    public HybridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            paginationDepth = in.readOptionalInt();
        }
        // Gate the fused-mode field on the actual peer stream version (not the cluster-min-version singleton) so the
        // read format matches exactly what the writing node wrote, regardless of singleton state at this instant.
        if (isVersionOnOrAfterMinReqVersionForFusedModeInHybridQuery(in.getVersion()) && in.readBoolean()) {
            fusion = in.readMap();
        }
    }

    /**
     * Serialize this query object into input stream
     * @param out stream that we'll be used for serialization
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            out.writeOptionalInt(paginationDepth);
        }
        // Gate the fused-mode field on the actual peer stream version so the wire format is symmetric with the reader.
        if (isVersionOnOrAfterMinReqVersionForFusedModeInHybridQuery(out.getVersion())) {
            // Presence flag then the map — absence writes only a false boolean, keeping the classic wire form compact.
            boolean hasFusion = Objects.nonNull(fusion);
            out.writeBoolean(hasFusion);
            if (hasFusion) {
                out.writeMap(fusion);
            }
        }
    }

    /**
     * Add one sub-query
     * @param queryBuilder
     * @return
     */
    public HybridQueryBuilder add(QueryBuilder queryBuilder) {
        if (Objects.isNull(queryBuilder)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "inner %s query clause cannot be null", NAME));
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * Function to support filter on HybridQueryBuilder filter.
     * If the filter is null, then we do nothing and return.
     * Otherwise, we push down the filter to queries list.
     * @param filter the filter parameter
     * @return HybridQueryBuilder itself
     */
    public QueryBuilder filter(QueryBuilder filter) {
        if (validateFilterParams(filter) == false) {
            return this;
        }
        ListIterator<QueryBuilder> iterator = queries.listIterator();
        while (iterator.hasNext()) {
            QueryBuilder query = iterator.next();
            // set the query again because query.filter(filter) can return new query.
            iterator.set(query.filter(filter));
        }
        return this;
    }

    /**
     * Create builder object with a content of this hybrid query
     * @param builder
     * @param params
     * @throws IOException
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        // TODO https://github.com/opensearch-project/neural-search/issues/1097
        if (Objects.nonNull(paginationDepth)) {
            builder.field(PAGINATION_DEPTH_FIELD.getPreferredName(), paginationDepth);
        }
        if (Objects.nonNull(fusion)) {
            builder.field(FUSION_FIELD.getPreferredName(), fusion);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Create query object for current hybrid query using shard context
     * @param queryShardContext context object that used to create hybrid query
     * @return hybrid query object
     * @throws IOException
     */
    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        // Safety net: fused mode self-erases at the coordinator (doRewriteFused) into a standard query, so a fused
        // builder must never reach a shard's doToQuery. If it does, the coordinator rewrite was skipped — fail loudly
        // rather than silently running classic scoring.
        if (Objects.nonNull(fusion)) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, ERROR_MSG_FUSION_REACHED_SHARD, NAME, FUSION_FIELD.getPreferredName())
            );
        }
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery(String.format(Locale.ROOT, "no clauses for %s query", NAME));
        }
        validatePaginationDepth(paginationDepth, queryShardContext);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(paginationDepth).build();
        return new HybridQuery(queryCollection, hybridQueryContext);
    }

    /**
     * Creates HybridQueryBuilder from xContent.
     * Example of a json for Hybrid Query:
     * {
     *      "query": {
     *          "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                                    "query_text": "Hello world",
     *                                    "model_id": "dcsdcasd",
     *                                    "k": 10
     *                                }
     *                            }
     *                   },
     *                   {
     *                      "term": {
     *                          "text": "keyword"
     *                       }
     *                    }
     *               ],
     *               "filter": {
     *                  "term": {
     *                      "text": "keyword"
     *                  }
     *               }
     *          }
     *     }
     * }
     *
     * To combine multiple filter clauses, wrap them in a bool query:
     * {
     *     "filter": {
     *         "bool": {
     *             "must": [
     *                 {"term": {"field1": "value1"}},
     *                 {"term": {"field2": "value2"}}
     *             ]
     *         }
     *     }
     * }
     *
     * @param parser parser that has been initialized with the query content
     * @return new instance of HybridQueryBuilder
     * @throws IOException
     */
    public static HybridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        Integer paginationDepth = null;
        Map<String, Object> fusion = null;
        final List<QueryBuilder> queries = new ArrayList<>();
        QueryBuilder filter = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queries.add(parseInnerQueryBuilder(parser));
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    filter = parseInnerQueryBuilder(parser);
                } else if (FUSION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fusion = parser.map();
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (queries.size() == MAX_NUMBER_OF_SUB_QUERIES) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(Locale.ROOT, ERROR_MSG_MAX_QUERIES_EXCEEDED, NAME)
                            );
                        }
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throwUnsupportedFilterParsingException(parser);
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                    // regular boost functionality is not supported, user should use score normalization methods to manipulate with scores
                    if (boost != DEFAULT_BOOST) {
                        log.error("[{}] query does not support provided value {} for [{}]", NAME, boost, BOOST_FIELD);
                        throw new ParsingException(parser.getTokenLocation(), "[{}] query does not support [{}]", NAME, BOOST_FIELD);
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (PAGINATION_DEPTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    paginationDepth = parser.intValue();
                } else if (FUSION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    // `"fusion": "pipeline"` == `"fusion": { "source": "pipeline" }`. Presence still
                    // enables the resolver; the config comes from the attached pipeline.
                    String source = parser.text();
                    if (FUSION_SOURCE_PIPELINE.equals(source) == false) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(
                                Locale.ROOT,
                                "[%s] query [%s] as a string must be [%s], got [%s]",
                                NAME,
                                FUSION_FIELD.getPreferredName(),
                                FUSION_SOURCE_PIPELINE,
                                source
                            )
                        );
                    }
                    Map<String, Object> normalized = new HashMap<>();
                    normalized.put(FUSION_KEY_SOURCE, source);
                    fusion = normalized;
                } else if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throwUnsupportedFilterParsingException(parser);
                } else {
                    throwUnsupportedFieldParsingException(parser, currentFieldName);
                }
            }
        }

        if (queries.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, ERROR_MSG_QUERIES_REQUIRED, NAME));
        }

        if (Objects.nonNull(fusion)) {
            validateFusion(fusion, paginationDepth, parser);
        }

        HybridQueryBuilder compoundQueryBuilder = new HybridQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        compoundQueryBuilder.fusion(fusion);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            compoundQueryBuilder.paginationDepth(paginationDepth);
        }

        boolean hasInnerHits = false;
        for (QueryBuilder query : queries) {
            if (Objects.isNull(filter)) {
                compoundQueryBuilder.add(query);
            } else {
                compoundQueryBuilder.add(query.filter(filter));
            }

            // Check if children have inner hits for stats
            if (hasInnerHits == false) {
                Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
                InnerHitContextBuilder.extractInnerHits(query, innerHits);
                hasInnerHits = innerHits.isEmpty() == false;
            }
        }

        boolean hasFilter = Objects.nonNull(filter);
        boolean hasPagination = Objects.nonNull(paginationDepth);
        updateQueryStats(hasFilter, hasPagination, hasInnerHits);
        return compoundQueryBuilder;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        // Resolver (fused) mode self-erases at the coordinator into a standard query (see doRewriteFused). Classic mode
        // keeps the existing per-sub-query rewrite below.
        if (Objects.nonNull(fusion)) {
            return doRewriteFused(queryShardContext);
        }
        HybridQueryBuilder newBuilder = new HybridQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
                newBuilder.paginationDepth(paginationDepth);
            }
            return newBuilder;
        } else {
            return this;
        }
    }

    /**
     * Coordinator-side self-erase for the resolver (fused) mode. Runs only on the coordinator (where
     * {@link QueryRewriteContext#convertToCoordinatorContext()} is non-null); on a shard it is a no-op and
     * {@link #doToQuery} throws, so the coordinator rewrite is the sole entry.
     *
     * <ul>
     *   <li><b>Round 1</b>: resolve the {@link FusionSpec} (inline block, else the attached pipeline), fire the legs as
     *       a parallel {@code MultiSearch} via {@link QueryRewriteContext#registerAsyncAction}, and return a marker
     *       carrying a {@link SetOnce}-backed supplier.</li>
     *   <li><b>Round 2</b>: the async action has produced the standard query — return it ({@link HybridFusionQuery} or
     *       {@code match_none}).</li>
     * </ul>
     */
    private QueryBuilder doRewriteFused(QueryRewriteContext queryRewriteContext) throws IOException {
        // Round 2: the async self-erase already produced the standard query — swap to it (or stay put until it lands).
        if (Objects.nonNull(fusedSupplier)) {
            QueryBuilder fused = fusedSupplier.get();
            return Objects.isNull(fused) ? this : fused;
        }
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (Objects.isNull(coordinatorContext)) {
            return this;
        }
        if ((coordinatorContext.getSearchRequest() instanceof SearchRequest) == false) {
            return this;
        }
        SearchRequest searchRequest = (SearchRequest) coordinatorContext.getSearchRequest();

        // Precedence: an inline `fusion` block on the query body wins; else resolve from the attached search pipeline
        // (inline body / named param / index default). Fail fast if neither yields a config rather than emitting
        // unfused scores.
        FusionSpec fusionSpec = Objects.nonNull(this.fusion) && hasInlineConfig(this.fusion)
            ? FusionSpec.fromInlineFusion(this.fusion)
            : FusionConfigResolver.resolve(searchRequest);
        if (Objects.isNull(fusionSpec)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] requires a normalization or score-ranker processor in the attached search pipeline "
                        + "(inline body, ?search_pipeline=, or index.search.default_pipeline), or an inline fusion block; none was found",
                    NAME,
                    FUSION_FIELD.getPreferredName()
                )
            );
        }
        // Current scope (first working slice): min_max + arithmetic_mean only. Other techniques parse but are not wired
        // into the coordinator fusion path yet — fail fast rather than silently mis-fuse.
        requireSupportedTechniques(fusionSpec);

        // Whether this query is the whole request (=> conditional Tail for accurate totals/aggs) or nested inside a
        // container (=> Top-only, so an enclosing filter intersects at the query phase).
        boolean topLevel = Objects.nonNull(searchRequest.source()) && searchRequest.source().query() == this;
        int window = effectiveWindowSize();
        // Each leg fires size=window per shard, so an unbounded window is a per-shard memory/CPU amplifier. Cap it at
        // index.max_result_window (resolved coordinator-side from the targeted indices), mirroring classic hybrid's
        // pagination_depth ceiling.
        validateWindowSizeAgainstMaxResultWindow(searchRequest, window);
        List<QueryBuilder> legs = queries;

        SetOnce<QueryBuilder> fused = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> client.multiSearch(
                HybridFusionOrchestrator.buildLegMultiSearch(searchRequest, legs, window),
                ActionListener.wrap(multiSearchResponse -> {
                    try {
                        fused.set(
                            HybridFusionOrchestrator.buildFusedQuery(
                                searchRequest.source(),
                                multiSearchResponse,
                                legs,
                                fusionSpec,
                                window,
                                topLevel
                            )
                        );
                        listener.onResponse(null);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure)
            )
        );

        HybridQueryBuilder marker = new HybridQueryBuilder();
        for (QueryBuilder query : queries) {
            marker.add(query);
        }
        marker.queryName(queryName);
        marker.boost(boost);
        marker.fusion(this.fusion);
        marker.fusedSupplier = fused::get;
        return marker;
    }

    /** True when the inline {@code fusion} block carries an actual config (not just {@code source: pipeline} / empty). */
    private static boolean hasInlineConfig(final Map<String, Object> fusion) {
        return fusion.containsKey(FUSION_KEY_NORMALIZATION) || fusion.containsKey(FUSION_KEY_COMBINATION);
    }

    /** Fail fast on techniques not yet wired into the coordinator fusion path (current scope: min_max + arithmetic_mean). */
    private static void requireSupportedTechniques(final FusionSpec fusionSpec) {
        boolean supported = FusionSpec.TECHNIQUE_ARITHMETIC_MEAN.equals(fusionSpec.combinationTechnique())
            && FusionSpec.NORMALIZATION_MIN_MAX.equals(fusionSpec.normalizationTechnique());
        if (supported == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] currently supports only normalization [%s] with combination [%s]; got normalization [%s], combination [%s]",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    FusionSpec.NORMALIZATION_MIN_MAX,
                    FusionSpec.TECHNIQUE_ARITHMETIC_MEAN,
                    fusionSpec.normalizationTechnique(),
                    fusionSpec.combinationTechnique()
                )
            );
        }
    }

    /** Fused-mode candidate window (top docs per leg), read from the {@code fusion.window_size} key, defaulting when unset. */
    private int effectiveWindowSize() {
        if (Objects.nonNull(fusion) && fusion.get(FUSION_KEY_WINDOW_SIZE) instanceof Number) {
            return ((Number) fusion.get(FUSION_KEY_WINDOW_SIZE)).intValue();
        }
        return DEFAULT_FUSION_WINDOW_SIZE;
    }

    /**
     * Reject a {@code window_size} above {@code index.max_result_window} for any targeted index — each leg fires
     * {@code size=window} per shard, so an unbounded window amplifies per-shard memory/CPU. Resolved coordinator-side
     * from the request's concrete indices (there is no shard {@link QueryShardContext} at rewrite), mirroring the
     * ceiling classic hybrid enforces on {@code pagination_depth}. A no-window (default) request is always within bounds.
     */
    private static void validateWindowSizeAgainstMaxResultWindow(final SearchRequest searchRequest, final int window) {
        for (IndexMetadata indexMetadata : NeuralSearchClusterUtil.instance().getIndexMetadataList(searchRequest)) {
            if (Objects.isNull(indexMetadata)) {
                continue;
            }
            int maxResultWindow = IndexSettings.MAX_RESULT_WINDOW_SETTING.get(indexMetadata.getSettings());
            if (window > maxResultWindow) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s.%s] (%d) must be less than or equal to [%s] (%d) for index [%s]",
                        NAME,
                        FUSION_FIELD.getPreferredName(),
                        FUSION_KEY_WINDOW_SIZE,
                        window,
                        IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                        maxResultWindow,
                        indexMetadata.getIndex().getName()
                    )
                );
            }
        }
    }

    /**
     * Indicates whether some other QueryBuilder object of the same type is "equal to" this one.
     * @param obj
     * @return true if objects are equal
     */
    @Override
    protected boolean doEquals(HybridQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj)) {
            return false;
        }
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queries, obj.queries);
        equalsBuilder.append(paginationDepth, obj.paginationDepth);
        equalsBuilder.append(fusion, obj.fusion);
        return equalsBuilder.isEquals();
    }

    /**
     * Create hash code for current hybrid query builder object
     * @return hash code
     */
    @Override
    protected int doHashCode() {
        return Objects.hash(queries, paginationDepth, fusion);
    }

    /**
     * Returns the name of the writeable object
     * @return
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    private List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        return in.readNamedWriteableList(QueryBuilder.class);
    }

    private void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeNamedWriteableList(queries);
    }

    private Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException {
        List<Query> queries = queryBuilders.stream().map(qb -> {
            try {
                return qb.rewrite(context).toQuery(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return queries;
    }

    /**
     * Parse-time (HTTP 400) structural validation of the {@code fusion} block. The value type (string|object) is already
     * enforced by the parser branches; this checks the object's internal consistency:
     * <ul>
     *   <li>unknown top-level key (outside {@code source|normalization|combination|window_size}) → 400;</li>
     *   <li>{@code source: "pipeline"} alongside inline {@code normalization}/{@code combination} → 400 (contradiction:
     *       read-from-pipeline vs inline config);</li>
     *   <li>{@code source} present but not {@code "pipeline"} → 400;</li>
     *   <li>{@code window_size} non-positive → 400 (upper bound vs {@code index.max_result_window} is shard-side);</li>
     *   <li>{@code pagination_depth} co-set with {@code fusion} → 400 (fused pages over {@code window_size}).</li>
     * </ul>
     */
    private static void validateFusion(final Map<String, Object> fusion, final Integer paginationDepth, final XContentParser parser) {
        for (String key : fusion.keySet()) {
            if (FUSION_KEY_SOURCE.equals(key) == false
                && FUSION_KEY_NORMALIZATION.equals(key) == false
                && FUSION_KEY_COMBINATION.equals(key) == false
                && FUSION_KEY_WINDOW_SIZE.equals(key) == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "[%s] query [%s] contains unknown key [%s]", NAME, FUSION_FIELD.getPreferredName(), key)
                );
            }
        }

        Object source = fusion.get(FUSION_KEY_SOURCE);
        if (Objects.nonNull(source)) {
            if (FUSION_SOURCE_PIPELINE.equals(source) == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s.%s] must be [%s]",
                        NAME,
                        FUSION_FIELD.getPreferredName(),
                        FUSION_KEY_SOURCE,
                        FUSION_SOURCE_PIPELINE
                    )
                );
            }
            if (fusion.containsKey(FUSION_KEY_NORMALIZATION) || fusion.containsKey(FUSION_KEY_COMBINATION)) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s] cannot combine [%s: %s] with inline [%s]/[%s]",
                        NAME,
                        FUSION_FIELD.getPreferredName(),
                        FUSION_KEY_SOURCE,
                        FUSION_SOURCE_PIPELINE,
                        FUSION_KEY_NORMALIZATION,
                        FUSION_KEY_COMBINATION
                    )
                );
            }
        }

        Object windowSize = fusion.get(FUSION_KEY_WINDOW_SIZE);
        if (Objects.nonNull(windowSize)) {
            if ((windowSize instanceof Number) == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s.%s] must be a positive integer",
                        NAME,
                        FUSION_FIELD.getPreferredName(),
                        FUSION_KEY_WINDOW_SIZE
                    )
                );
            }
            if (((Number) windowSize).intValue() <= 0) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "[%s] query [%s.%s] must be greater than 0",
                        NAME,
                        FUSION_FIELD.getPreferredName(),
                        FUSION_KEY_WINDOW_SIZE
                    )
                );
            }
        }

        if (Objects.nonNull(paginationDepth)) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query does not support [%s] together with [%s]; fused mode pages over [%s.%s]",
                    NAME,
                    PAGINATION_DEPTH_FIELD.getPreferredName(),
                    FUSION_FIELD.getPreferredName(),
                    FUSION_FIELD.getPreferredName(),
                    FUSION_KEY_WINDOW_SIZE
                )
            );
        }
    }

    private static void validatePaginationDepth(final Integer paginationDepth, final QueryShardContext queryShardContext) {
        if (Objects.isNull(paginationDepth)) {
            return;
        }
        if (paginationDepth < LOWER_BOUND_OF_PAGINATION_DEPTH) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "pagination_depth should be greater than %s", LOWER_BOUND_OF_PAGINATION_DEPTH)
            );
        }
        // compare pagination depth with OpenSearch setting index.max_result_window
        // see https://opensearch.org/docs/latest/install-and-configure/configuring-opensearch/index-settings/
        int maxResultWindowIndexSetting = queryShardContext.getIndexSettings().getMaxResultWindow();
        if (paginationDepth > maxResultWindowIndexSetting) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "pagination_depth should be less than or equal to %s setting",
                    IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                )
            );
        }
    }

    /**
     * visit method to parse the HybridQueryBuilder by a visitor
     */
    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        // getChildVisitor of NeuralSearchQueryVisitor return this.
        // therefore any argument can be passed. Here we have used Occcur.MUST as an argument.
        QueryBuilderVisitor subVisitor = visitor.getChildVisitor(Occur.MUST);
        for (QueryBuilder subQueryBuilder : queries) {
            subQueryBuilder.visit(subVisitor);
        }
    }

    /**
     * Extracts the inner hits from the hybrid query tree structure.
     * While it extracts inner hits, child inner hits are inlined into the inner hit builder they belong to.
     * This implementation handles inner hits for all sub-queries within the hybrid query.
     *
     * @param innerHits the map to collect inner hit contexts, where the key is the inner hit name
     *                   and the value is the corresponding inner hit context builder
     */
    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        for (QueryBuilder queryBuilder : queries) {
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHits);
        }
    }

    public static void updateQueryStats(boolean hasFilter, boolean hasPagination, boolean hasInnerHits) {
        EventStatsManager.increment(EventStatName.HYBRID_QUERY_REQUESTS);
        if (hasFilter) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_FILTER_REQUESTS);
        }
        if (hasPagination) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_PAGINATION_REQUESTS);
        }
        if (hasInnerHits) {
            EventStatsManager.increment(EventStatName.HYBRID_QUERY_INNER_HITS_REQUESTS);
        }
    }

    private static void throwUnsupportedFilterParsingException(XContentParser parser) {
        throw new ParsingException(
            parser.getTokenLocation(),
            String.format(Locale.ROOT, ERROR_MSG_FILTER_MUST_BE_QUERY_OBJECT, NAME, FILTER_FIELD.getPreferredName())
        );
    }

    private static void throwUnsupportedFieldParsingException(XContentParser parser, String fieldName) {
        log.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, fieldName));
        throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME));
    }
}
