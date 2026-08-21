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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.search.SearchService;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.fusion.ScalarNormalizer;
import org.opensearch.neuralsearch.fusion.ScalarNormalizers;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY;
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
     * form). A string {@code "pipeline"} is normalized to {@code {source: "pipeline"}} at parse.
     *
     * <p>A well-formed {@code fusion} block drives execution: {@link #doRewrite} routes to {@link #doRewriteFused}, which
     * fans the legs out and self-erases into a standard query on the coordinator.
     *
     * <p>Serialized over the transport wire behind a peer-stream-version gate, so a request carrying {@code fusion} keeps
     * a wire form a pre-fused-mode node can still read: the field is written only when the peer understands it, and its
     * absence costs a single {@code false} boolean.
     */
    private Map<String, Object> fusion;

    /**
     * Round-1 → round-2 bridge for the resolver (fused) mode. The async leg {@code MultiSearch} registered in
     * {@link #doRewriteFused} sets the self-erased standard query here; the next rewrite round returns it. {@code null}
     * on a freshly parsed builder (classic path never touches it).
     */
    private Supplier<QueryBuilder> fusedSupplier;

    /**
     * Fusion config projected onto this builder by an <b>enclosing</b> fused hybrid, when this builder is one of its legs.
     * A leg sub-search runs with the search pipeline pinned to {@code _none} (see
     * {@link HybridFusionOrchestrator#buildLegMultiSearch}), so a nested fused hybrid cannot resolve its own config from
     * the leg request; the enclosing rewrite hands down the config it already resolved from the user's request instead.
     * Only ever set on a private copy made by {@link #withResolvedFusionSpec}, never parsed, never serialized, and
     * deliberately absent from {@link #doEquals}/{@link #doHashCode} — exactly like {@link #fusedSupplier}.
     */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private FusionSpec resolvedFusionSpec;

    public static final int MAX_NUMBER_OF_SUB_QUERIES = 5;
    private static final int LOWER_BOUND_OF_PAGINATION_DEPTH = 0;
    private static final int DEFAULT_FUSION_WINDOW_SIZE = 100;
    /** The one clause the Tail filter takes in the self-erased bool, alongside one should-clause per ranked doc. */
    private static final int TAIL_CLAUSE_RESERVE = 1;

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
     *   <li><b>Round 2</b>: the async action has produced the standard query — return it ({@link HybridFusionQueryBuilder} or
     *       {@code match_none}).</li>
     * </ul>
     */
    private QueryBuilder doRewriteFused(QueryRewriteContext queryRewriteContext) throws IOException {
        // Only the match set is wanted (this builder is a leg inside an enclosing fused query's Tail): contribute what the
        // legs match and do not fan them out again — they already ran, as the enclosing query's leg sub-search. Checked
        // ahead of everything else: it is the one case where the correct answer is to do no work at all.
        if (MatchSetRewriteContext.isMatchSetOnly(queryRewriteContext)) {
            return matchSetQuery();
        }
        // Round 2: the async self-erase already produced the standard query — swap to it (or stay put until it lands).
        if (Objects.nonNull(fusedSupplier)) {
            QueryBuilder fused = fusedSupplier.get();
            return Objects.isNull(fused) ? this : fused;
        }
        QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (Objects.isNull(coordinatorContext)) {
            return this;
        }
        // Before anything about the request itself: a cluster that cannot run round 2 on every shard cannot run fused mode
        // at all. Ahead of the SearchRequest cast on purpose — _explain and _validate/query rewrite on the coordinator with
        // a non-search request and are dispatched still-fused, so they need the same refusal.
        requireClusterSupportsFusedMode();
        if ((coordinatorContext.getSearchRequest() instanceof SearchRequest) == false) {
            return this;
        }
        SearchRequest searchRequest = (SearchRequest) coordinatorContext.getSearchRequest();

        // First, before any config resolution: the whole request's fan-out has to be within the cluster's budget. A body
        // whose only purpose is to multiply sub-searches should cost one tree walk, not a pipeline lookup per hybrid.
        validateFusedLegSearchBudget(searchRequest);
        // Then the request's own shape, in one place: what each leg inherits, and a refusal for the shapes fused mode
        // cannot answer correctly. Ahead of every check that resolves index metadata — config resolution reads the
        // targeted indices' default pipelines, and the window ceiling reads their max_result_window — because a request
        // naming a remote index dies in index resolution with `no such index [cluster:index]`, which would hide this
        // class's explanation of why fused mode refuses cross-cluster search at all.
        CandidateScope candidateScope = CandidateScope.from(searchRequest);

        FusionSpec fusionSpec = resolveFusionSpec(searchRequest);
        if (Objects.isNull(fusionSpec)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] requires a normalization or score-ranker processor: the resolved search pipeline "
                        + "(from ?search_pipeline= or index.search.default_pipeline) has none, and no inline fusion block "
                        + "was provided (a missing pipeline id is rejected earlier by core). A fused [%s] nested inside "
                        + "another compound query (for example [bool] or [dis_max]) within a leg of an enclosing fused [%s] "
                        + "must carry an inline [%s] config, because a leg sub-search runs with the search pipeline disabled",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    NAME,
                    NAME,
                    FUSION_FIELD.getPreferredName()
                )
            );
        }
        // Current scope: the whole score-normalization family (min_max, z_score, l2) combined by arithmetic_mean. Other
        // techniques parse but are not wired into the coordinator fusion path yet — fail fast rather than mis-fuse.
        requireSupportedTechniques(fusionSpec);

        int window = effectiveWindowSize();
        // Each leg fires size=window per shard, so an unbounded window is a per-shard memory/CPU amplifier. Cap it at
        // index.max_result_window (resolved coordinator-side from the targeted indices), mirroring classic hybrid's
        // pagination_depth ceiling.
        validateWindowSizeAgainstMaxResultWindow(searchRequest, window);
        // The self-erased query holds one bool clause per ranked doc, so the window is also bounded by Lucene's clause
        // ceiling — a different (and much lower) limit than max_result_window, and the only one that would otherwise be
        // discovered at query time on every shard.
        validateWindowSizeAgainstMaxClauseCount(window);
        List<QueryBuilder> legs = queries;
        // Validate weights (range, sum, count) before the leg fan-out — a bad weights array otherwise burns a full
        // MultiSearch before the combiner is built in the async callback.
        HybridFusionOrchestrator.validateFusionParams(fusionSpec, legs.size());
        // The Tail keeps the original legs (it is rewritten against the user's request, which still carries the
        // pipeline), but the fanned-out legs run with the pipeline disabled — so hand the resolved config down.
        List<QueryBuilder> fanOutLegs = projectResolvedConfigOntoLegs(legs, fusionSpec);

        SetOnce<QueryBuilder> fused = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> client.multiSearch(
                HybridFusionOrchestrator.buildLegMultiSearch(candidateScope, fanOutLegs, window),
                ActionListener.wrap(multiSearchResponse -> {
                    try {
                        fused.set(
                            HybridFusionOrchestrator.buildFusedQuery(searchRequest.source(), multiSearchResponse, legs, fusionSpec, window)
                        );
                        listener.onResponse(null);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                    // Whole-MultiSearch transport failure (cancellation, rejection, coordinator error) — not a per-leg
                    // Item failure. Frame it as the user's hybrid/fused query rather than surfacing a bare multiSearch
                    // error, but keep the underlying status: a cancellation, a rejection and a coordinator bug are three
                    // different answers, and IllegalStateException collapses all of them to 500.
                },
                    e -> listener.onFailure(
                        new OpenSearchStatusException(
                            String.format(
                                Locale.ROOT,
                                "[%s] query [%s] failed to execute fused-mode sub-queries: %s",
                                NAME,
                                FUSION_FIELD.getPreferredName(),
                                e.getMessage()
                            ),
                            ExceptionsHelper.status(e),
                            e
                        )
                    )
                )
            )
        );

        HybridQueryBuilder marker = new HybridQueryBuilder();
        for (QueryBuilder query : queries) {
            marker.add(query);
        }
        marker.queryName(queryName);
        marker.boost(boost);
        marker.fusion(this.fusion);
        marker.resolvedFusionSpec = this.resolvedFusionSpec;
        marker.fusedSupplier = fused::get;
        return marker;
    }

    /**
     * This query's match set, as a plain {@code bool{should: legs}} that costs no fan-out.
     *
     * <p>Equal to what the fused query matches, clause for clause. A fused hybrid compiles to
     * {@code bool{ should: [Top...], filter: Tail }} with no {@code minimum_should_match}, so its matches are its Tail's —
     * and the Tail is {@code bool{should: legs}}, the union of the legs. The scores differ (this loses them entirely), which
     * is exactly why this form is only ever used where scores are not read: inside the enclosing query's Tail
     * {@code filter}. A doc that this bool matches while the real fused query would not (or the reverse) does not exist.
     *
     * <p>The legs are handed over as-is rather than rewritten here — the caller's rewrite loop keeps going. The one
     * exception is a leg that is itself a fused hybrid: it contributes <i>its</i> match set in this same pass, rather than
     * being left for the next rewrite round. Rewrite descends one level per round ({@code BoolQueryBuilder} rewrites each
     * clause exactly once), and core allows {@link org.opensearch.index.query.Rewriteable#MAX_REWRITE_ROUNDS} rounds for the
     * whole request, so substituting level by level would spend a round per level of nesting — a chain the leg budget admits
     * would exhaust them and fail as an internal error instead of running. Collapsing the chain here makes the cost of a
     * nested chain constant in rounds. Nesting reached through a container leg (a fused hybrid inside a {@code bool} inside
     * a leg) is still substituted a level per round, since only core can rewrite core's containers.
     *
     * <p>{@code match_none} for a legless builder: {@code bool{should: []}} compiles to a {@code MatchAllDocsQuery}, which
     * in a Tail {@code filter} would open the match set to the whole corpus (the same trap
     * {@code HybridFusionOrchestrator#materializedLeg} guards for an empty ANN leg). Parsing rejects an empty
     * {@code queries} array, so this is unreachable from a request — and stated rather than relied upon.
     */
    private QueryBuilder matchSetQuery() {
        if (queries.isEmpty()) {
            return new MatchNoneQueryBuilder();
        }
        BoolQueryBuilder matchSet = new BoolQueryBuilder();
        for (QueryBuilder query : queries) {
            boolean legIsFusedHybrid = query instanceof HybridQueryBuilder && Objects.nonNull(((HybridQueryBuilder) query).fusion());
            matchSet.should(legIsFusedHybrid ? ((HybridQueryBuilder) query).matchSetQuery() : query);
        }
        matchSet.queryName(queryName);
        matchSet.boost(boost);
        return matchSet;
    }

    /**
     * Refuse fused mode while any node in the cluster predates it.
     *
     * <p>Round 2 dispatches a {@link HybridFusionQueryBuilder} — a query type introduced together with fused mode — to
     * every shard the request touches, and a node predating that version cannot resolve the {@code hybrid_fusion}
     * {@link org.opensearch.core.common.io.stream.NamedWriteable} name at all, so it fails while deserializing the shard
     * request. That is not a clean error to the client: with {@code allow_partial_search_results} at its default the
     * request still answers 200, with every document on those shards silently missing. A leg fan-out that reached an old
     * node fails the same way, and a still-fused builder dispatched by {@code _explain} / {@code _validate/query} is worse
     * — the {@code fusion} field is gated off the wire, so the old node answers for a <i>classic</i> hybrid instead.
     *
     * <p>Cluster-wide minimum here, where the wire gates on the {@code fusion} field ({@link #doWriteTo} and the
     * {@link StreamInput} constructor) use the peer stream's negotiated version: those pick a serialization format for one
     * known peer, this decides whether the coordinator may enter a path whose recipients are not known yet. Checked at
     * rewrite on the coordinator, before the fan-out, so the refusal costs less than the search it replaces.
     *
     * <p>Over-refuses in one shape — a cluster whose only lagging node holds none of the targeted shards, a
     * cluster-manager-only or coordinating-only node — and that is the intended trade: routing is not knowable at rewrite,
     * and the alternative is answering some of those requests with results silently missing.
     */
    private static void requireClusterSupportsFusedMode() {
        Version clusterMinVersion = NeuralSearchClusterUtil.instance().getClusterMinVersion();
        if (isVersionOnOrAfterMinReqVersionForFusedModeInHybridQuery(clusterMinVersion) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] requires all nodes in the cluster to be on version [%s] or later, but the minimum node "
                        + "version is [%s], so the fused query cannot be dispatched to every shard. Upgrade the remaining "
                        + "nodes, or drop the [%s] block and use classic hybrid with a normalization or score-ranker processor",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    MINIMAL_SUPPORTED_VERSION_FUSED_MODE_IN_HYBRID_QUERY,
                    clusterMinVersion,
                    FUSION_FIELD.getPreferredName()
                )
            );
        }
    }

    /**
     * Reject a request that would fan out more leg sub-searches than the cluster allows.
     *
     * <p>The counted quantity is <b>declared leg sub-searches</b> — the sum of {@code queries} sizes over every fused
     * {@code hybrid} in the request body — deliberately modelled on {@code indices.query.bool.max_clause_count}: one
     * whole-request count of the units the user actually wrote, checked once at rewrite, against a dynamic cluster
     * setting. Shards are not a factor. A user cannot be asked to reason about legs × shards × nesting: the shard count
     * is a property of the indices they searched, not of the query they wrote, so folding it in would make the same body
     * legal on one cluster and rejected on another, and the number in the error message unreproducible.
     *
     * <p>The default, {@value org.opensearch.neuralsearch.settings.NeuralSearchSettings#DEFAULT_MAX_FUSION_LEG_SEARCHES},
     * is {@link #MAX_NUMBER_OF_SUB_QUERIES} squared — a hybrid may declare 5 legs, so 5 levels of fully-nested hybrids is
     * the shape this admits, and the setting's own floor is {@code MAX_NUMBER_OF_SUB_QUERIES} so that a legal single-level
     * hybrid can never be rejected by it.
     *
     * <p>Counted from the request's own query, not from this builder, so every fused hybrid in a request agrees on one
     * number: sibling hybrids in a {@code bool} each pay for the others, which is the point — the request is what fans
     * out. Nesting is walked through {@link QueryBuilder#visit}, so the count is exact for the query builders that expose
     * their children (all of core's compound queries do) and a lower bound for one that does not — a {@code wrapper} query
     * carries its inner query as bytes, so a hybrid hidden inside one is invisible here and is counted when the leg
     * sub-search carrying it rewrites on its own coordinator. That leaves total cost linear in the size of the body the
     * user had to spell out, which is the property this guard exists to keep.
     */
    private static void validateFusedLegSearchBudget(final SearchRequest searchRequest) {
        if (Objects.isNull(searchRequest.source())) {
            return;
        }
        int maxLegSearches = maxFusionLegSearches();
        int legSearches = countFusedLegSearches(searchRequest.source().query());
        if (legSearches > maxLegSearches) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] declares %d leg sub-searches in this request, more than [%s] (%d). Each leg of each "
                        + "fused [%s] runs as its own search across the targeted shards; reduce the number of legs or of "
                        + "nested fused [%s] queries, or raise the setting",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    legSearches,
                    MAX_FUSION_LEG_SEARCHES.getKey(),
                    maxLegSearches,
                    NAME,
                    NAME
                )
            );
        }
    }

    /**
     * The live value of {@link NeuralSearchSettings#MAX_FUSION_LEG_SEARCHES} on this coordinator. Read from
     * {@link org.opensearch.cluster.service.ClusterService}'s cluster settings so a dynamic update takes effect on the
     * next request; falls back to the default when there is no cluster service to read (no running node — a guardrail
     * must not become a new way to fail).
     */
    private static int maxFusionLegSearches() {
        ClusterService clusterService = NeuralSearchClusterUtil.instance().getClusterService();
        if (Objects.isNull(clusterService) || Objects.isNull(clusterService.getClusterSettings())) {
            return MAX_FUSION_LEG_SEARCHES.getDefault(Settings.EMPTY);
        }
        return clusterService.getClusterSettings().get(MAX_FUSION_LEG_SEARCHES);
    }

    /**
     * Total leg sub-searches the given query declares: the legs of every fused {@code hybrid} in the tree, itself
     * included. Package-private so the count can be asserted per query shape without a cluster.
     */
    static int countFusedLegSearches(final QueryBuilder query) {
        if (Objects.isNull(query)) {
            return 0;
        }
        LegSearchCounter counter = new LegSearchCounter();
        query.visit(counter);
        return counter.legSearches;
    }

    /**
     * Counts the legs of every fused hybrid it is shown. One instance walks the whole tree — it hands itself back as the
     * child visitor, and {@link #visit} recurses into the legs — so a nested fused hybrid is counted at every depth the
     * builders expose.
     */
    private static final class LegSearchCounter implements QueryBuilderVisitor {
        private int legSearches;

        @Override
        public void accept(final QueryBuilder queryBuilder) {
            if (queryBuilder instanceof HybridQueryBuilder && Objects.nonNull(((HybridQueryBuilder) queryBuilder).fusion())) {
                legSearches += ((HybridQueryBuilder) queryBuilder).queries().size();
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(final Occur occur) {
            return this;
        }
    }

    /**
     * Resolve the fusion config for this rewrite, in precedence order:
     * <ol>
     *   <li>an inline {@code fusion} block on this query body (explicit user intent, wins outright);</li>
     *   <li>a config projected down by an enclosing fused hybrid ({@link #resolvedFusionSpec}) — this builder is one of
     *       its legs, and the leg request has no pipeline to read;</li>
     *   <li>the search pipeline attached to the request (inline body / {@code ?search_pipeline=} / index default).</li>
     * </ol>
     * {@code null} when none of the three yields a config; the caller fails fast rather than emitting unfused scores.
     */
    private FusionSpec resolveFusionSpec(final SearchRequest searchRequest) {
        if (Objects.nonNull(this.fusion) && hasInlineConfig(this.fusion)) {
            return FusionSpec.fromInlineFusion(this.fusion);
        }
        if (Objects.nonNull(resolvedFusionSpec)) {
            return resolvedFusionSpec;
        }
        return FusionConfigResolver.resolve(searchRequest);
    }

    /**
     * Hand the already-resolved fusion config down to any leg that is itself a fused hybrid without an inline config.
     *
     * <p>Legs are fanned out with {@code pipeline=_none} so that per-leg request/response processors do not run
     * ({@link HybridFusionOrchestrator#buildLegMultiSearch}); a nested fused hybrid would therefore resolve no config
     * from its own leg request and fail with a message blaming a pipeline the user has correctly configured. Projecting
     * the enclosing config is faithful: resolving from the pipeline is exactly what the nested query would have done,
     * and it is the same request and therefore the same pipeline.
     *
     * <p>Reaches direct legs only, at any nesting depth (each level projects onto its own legs). A fused hybrid buried
     * inside a container query within a leg (e.g. {@code bool{must: hybrid{fusion}}}) is not reachable —
     * {@link QueryBuilder} exposes no generic child accessor — and still fails, now with a message that names the real
     * cause and the inline-config workaround.
     *
     * @return the original list when nothing needed projecting (the common case), else a copy with legs substituted
     */
    static List<QueryBuilder> projectResolvedConfigOntoLegs(final List<QueryBuilder> legs, final FusionSpec resolved) {
        List<QueryBuilder> projected = null;
        for (int i = 0; i < legs.size(); i++) {
            QueryBuilder leg = legs.get(i);
            if ((leg instanceof HybridQueryBuilder) == false) {
                continue;
            }
            HybridQueryBuilder nested = (HybridQueryBuilder) leg;
            if (Objects.isNull(nested.fusion) || hasInlineConfig(nested.fusion)) {
                continue;
            }
            if (Objects.isNull(projected)) {
                projected = new ArrayList<>(legs);
            }
            projected.set(i, nested.withResolvedFusionSpec(resolved));
        }
        return Objects.isNull(projected) ? legs : projected;
    }

    /**
     * Copy of this builder carrying a fusion config resolved by an enclosing fused hybrid. A copy rather than in-place
     * mutation because the same leg instance is also reused for the Tail, and because {@code fusion} participates in
     * {@link #doEquals}.
     */
    private HybridQueryBuilder withResolvedFusionSpec(final FusionSpec resolved) {
        HybridQueryBuilder copy = new HybridQueryBuilder();
        for (QueryBuilder query : queries) {
            copy.add(query);
        }
        copy.queryName(queryName);
        copy.boost(boost);
        // Always null on a fused builder (`pagination_depth` with `fusion` is a parse-time 400), copied for exactness.
        copy.paginationDepth(paginationDepth);
        copy.fusion(fusion);
        copy.resolvedFusionSpec = resolved;
        return copy;
    }

    /** True when the inline {@code fusion} block carries an actual config (not just {@code source: pipeline} / empty). */
    private static boolean hasInlineConfig(final Map<String, Object> fusion) {
        return fusion.containsKey(FUSION_KEY_NORMALIZATION) || fusion.containsKey(FUSION_KEY_COMBINATION);
    }

    /**
     * Combination techniques wired into the coordinator fusion path. Narrower than the classic compatibility matrix
     * while combination support is widened one technique at a time; widening this set is what admits a new combiner.
     */
    private static final Set<String> FUSED_COMBINATION_TECHNIQUES = Set.of(FusionSpec.TECHNIQUE_ARITHMETIC_MEAN);

    /**
     * Fail fast on fusion configs the coordinator path cannot honor, rather than silently mis-fusing. Three checks,
     * ordered so that whichever one fires names the blocker a user can act on:
     * <ol>
     *   <li>the combination technique is inside fused mode's current scope;</li>
     *   <li>the normalization technique has a coordinator-side {@link ScalarNormalizer};</li>
     *   <li>the pairing is one the classic path allows, read from the same matrix classic enforces.</li>
     * </ol>
     */
    private static void requireSupportedTechniques(final FusionSpec fusionSpec) {
        final String normalization = fusionSpec.normalizationTechnique();
        final String combination = fusionSpec.combinationTechnique();

        // Combination first, because that is where RRF stops: it is modelled as combination=rrf with normalization=none,
        // so checking the normalizer registry first would blame [none] instead of naming rrf. Rank-based fusion needs
        // its own routing rather than a ScalarNormalizer.
        if (FUSED_COMBINATION_TECHNIQUES.contains(combination) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] does not support combination [%s] in fused mode; supported combinations are %s",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    combination,
                    new TreeSet<>(FUSED_COMBINATION_TECHNIQUES)
                )
            );
        }

        if (ScalarNormalizers.supportedTechniques().contains(normalization) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] does not support normalization [%s] in fused mode; supported normalizations are %s",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    normalization,
                    new TreeSet<>(ScalarNormalizers.supportedTechniques())
                )
            );
        }

        // Defer to the one compatibility matrix the classic path enforces, so a pairing classic rejects cannot slip in
        // through fused mode — z_score with geometric or harmonic mean being the case that exists today. Not yet
        // load-bearing, because the scope check above admits only arithmetic_mean and every normalization accepts that;
        // it starts biting as soon as that scope widens.
        final Set<String> classicPairings = ScoreNormalizationFactory.supportedCombinationTechniques(normalization);
        if (classicPairings.contains(combination) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s] does not support combination [%s] with normalization [%s]; supported combinations "
                        + "for that normalization are %s",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    combination,
                    normalization,
                    new TreeSet<>(classicPairings)
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
     * Reject a {@code window_size} the self-erased query could not be assembled from. Its Top is one {@code should}
     * clause per ranked document in a single {@code bool}, plus one {@code filter} clause when the Tail is present, and
     * {@code BooleanQuery.Builder#add} throws {@code TooManyClauses} as soon as one bool exceeds
     * {@code indices.query.bool.max_clause_count}. That ceiling defaults to 1024 and is unrelated to
     * {@code index.max_result_window} (default 10000) — the only bound the other window check applies — so without this a
     * {@code window_size} in the thousands parses, fans out, fuses, and only then fails on every shard. The setting is a
     * dynamic node setting that {@code SearchService} keeps in sync with Lucene's static, so reading the static here
     * reflects the live value on this coordinator.
     *
     * <p>Necessary, not sufficient: the Tail's own clauses are the user's legs and cannot be counted at rewrite, so a
     * request within this bound can still exceed the ceiling through an enormous leg query. The {@code _index}
     * qualification on each Top clause costs nothing against this bound — on the shard's own index the {@code _index}
     * filter is a MatchAll that {@code BooleanQuery.rewrite} removes, collapsing the clause back to
     * {@code constant_score(ids)}.
     */
    private static void validateWindowSizeAgainstMaxClauseCount(final int window) {
        int maxClauseCount = IndexSearcher.getMaxClauseCount();
        // Reserve the one clause the Tail filter occupies in the same bool. Whether a request needs the Tail is only known
        // once the legs have answered, so the window has to fit either way.
        if (window > maxClauseCount - TAIL_CLAUSE_RESERVE) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] query [%s.%s] (%d) must be less than [%s] (%d): the fused query holds one clause per ranked "
                        + "document plus one for the tail",
                    NAME,
                    FUSION_FIELD.getPreferredName(),
                    FUSION_KEY_WINDOW_SIZE,
                    window,
                    SearchService.INDICES_MAX_CLAUSE_COUNT_SETTING.getKey(),
                    maxClauseCount
                )
            );
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
