/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Objects;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Runs a classic hybrid query that no search pipeline would combine in fused mode with the default techniques.
 *
 * <p>A classic hybrid (one without {@code fusion}) is combined only by a {@code normalization-processor} or a
 * {@code score-ranker-processor} in the request's search pipeline. Without one nothing turns each shard's per-sub-query
 * results into a ranking, and the response carries them as they are: delimiter pseudo-hits, duplicated documents and raw,
 * unsorted scores. When the request's top-level query is such a hybrid, {@link #applyTo} hands back a copy of the request
 * whose query runs in fused mode with the default techniques — {@code min_max} normalization and {@code arithmetic_mean}
 * combination, the defaults of {@code normalization-processor} — exactly as if the hybrid carried {@code fusion: {}}.
 *
 * <p>Decided once per search request on the coordinating node, by {@code HybridQuerySearchRequestFilter}, before core
 * resolves the search pipeline. The request stays classic, with today's behavior, where fused mode has nothing to repair or
 * cannot run:
 * <ul>
 *   <li>{@code size: 0}, {@code _count} included: there are no hits to combine, and totals and aggregations stay the ones a
 *       search pipeline would leave;</li>
 *   <li>{@code scroll}, which both modes refuse; a remote index, or a cross-cluster sub-request arriving from another
 *       cluster, because fused mode does not search across clusters;</li>
 *   <li>a {@code pagination_depth} of zero or less, which classic refuses on the shard;</li>
 *   <li>fused mode switched off ({@code plugins.neural_search.hybrid.fusion.enabled}), a node older than fused mode, and
 *       anything that cannot be decided.</li>
 * </ul>
 * A request shape that fused mode refuses, {@code terminate_after} for example, runs with the default fusion anyway and is
 * refused with a message that says why fused mode ran.
 */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DefaultFusion {

    /** {@code SearchRequest#getLocalClusterAlias}, package-private in core; null when it cannot be read. */
    private static final MethodHandle LOCAL_CLUSTER_ALIAS = searchRequestGetter("getLocalClusterAlias");

    /**
     * The request to run. When the request's top-level query is a classic hybrid that no search pipeline would combine, a copy
     * of the request whose source holds a copy of that hybrid carrying {@code fusion: {}}; otherwise the request itself. The
     * caller's request, source and query are never written, so a request reused or dispatched again is decided again from
     * what its caller holds. Never throws: whatever cannot be decided leaves the request as it is.
     */
    public static SearchRequest applyTo(final SearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (Objects.isNull(source) || (source.query() instanceof HybridQueryBuilder) == false) {
            return request;
        }
        HybridQueryBuilder hybrid = (HybridQueryBuilder) source.query();
        if (Objects.nonNull(hybrid.fusion()) || runsWithDefaultFusion(request, source, hybrid) == false) {
            return request;
        }
        SearchRequest withDefaultFusion = new SearchRequest(request).source(copyWithDefaultFusion(source, hybrid));
        // The copy constructor leaves out the named search pipeline and the transport-level state; carried over here.
        withDefaultFusion.pipeline(request.pipeline());
        withDefaultFusion.setParentTask(request.getParentTask());
        withDefaultFusion.remoteAddress(request.remoteAddress());
        log.debug(
            "[{}] query without [fusion] runs in fusion mode with the default techniques: no [{}] or [{}] applies to the request",
            HybridQueryBuilder.NAME,
            NormalizationProcessor.TYPE,
            RRFProcessor.TYPE
        );
        return withDefaultFusion;
    }

    private static boolean runsWithDefaultFusion(
        final SearchRequest request,
        final SearchSourceBuilder source,
        final HybridQueryBuilder hybrid
    ) {
        try {
            if (hybrid.queries().isEmpty()
                || source.size() == 0
                || Objects.nonNull(request.scroll())
                || CandidateScope.targetsRemoteCluster(request)
                || isCrossClusterSubRequest(request)
                || (Objects.nonNull(hybrid.paginationDepth()) && hybrid.paginationDepth() <= 0)
                // The operator's switch ahead of the cluster's capability, as in fused mode's own checks.
                || HybridQueryBuilder.fusedModeEnabled() == false
                || Objects.isNull(NeuralSearchClusterUtil.instance().getClusterService())
                || HybridQueryBuilder.clusterSupportsFusedMode() == false) {
                return false;
            }
            return FusionConfigResolver.resolvedPipelineHasScoreHybridizationProcessor(request) == false;
        } catch (Exception e) {
            log.debug(
                "[{}] query without [fusion] stays in classic mode: deciding whether a search pipeline combines its scores failed",
                HybridQueryBuilder.NAME,
                e
            );
            return false;
        }
    }

    /**
     * {@code shallowCopy()} is what core's own rewrite copies a source with; it leaves out only the inline search pipeline
     * body, which core has not read yet at this point, so the copy gets it back. Fused mode confines the rescore list in place
     * ({@code FusedRescoreScope#install}), so the copy also gets a rescore list of its own and the caller's stays as written.
     */
    private static SearchSourceBuilder copyWithDefaultFusion(final SearchSourceBuilder submitted, final HybridQueryBuilder hybrid) {
        SearchSourceBuilder copy = submitted.shallowCopy().searchPipelineSource(submitted.searchPipelineSource());
        if (Objects.nonNull(submitted.rescores())) {
            copy.clearRescorers();
            submitted.rescores().forEach(copy::addRescorer);
        }
        return copy.query(hybrid.withDefaultFusion(submitted));
    }

    /**
     * Whether the request is core's copy for one remote cluster of a cross-cluster search that minimizes round trips
     * ({@code SearchRequest#subSearchRequest}): it names that cluster's indices locally and carries no search pipeline, so
     * nothing else tells it apart from a request a user sent to this cluster. Core marks it with {@code localClusterAlias},
     * behind a package-private getter, read here reflectively as the security plugin reads it. Fails closed: a getter that
     * cannot be read counts as a sub-request, which keeps every hybrid query on this node classic.
     */
    static boolean isCrossClusterSubRequest(final SearchRequest request) {
        return isCrossClusterSubRequest(request, LOCAL_CLUSTER_ALIAS);
    }

    /** As {@link #isCrossClusterSubRequest(SearchRequest)}, reading core's mark through {@code localClusterAlias}. */
    static boolean isCrossClusterSubRequest(final SearchRequest request, final MethodHandle localClusterAlias) {
        if (Objects.isNull(localClusterAlias)) {
            return true;
        }
        try {
            return Objects.nonNull((String) localClusterAlias.invokeExact(request));
        } catch (Throwable t) {
            return true;
        }
    }

    /** A handle on the no-argument {@code SearchRequest} method of that name, or null when it cannot be read. */
    @SuppressForbidden(reason = "SearchRequest#getLocalClusterAlias is package-private and is the only mark of a cross-cluster sub-request")
    static MethodHandle searchRequestGetter(final String name) {
        try {
            Method getter = SearchRequest.class.getDeclaredMethod(name);
            getter.setAccessible(true);
            return MethodHandles.lookup().unreflect(getter);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn(
                "cannot read SearchRequest#{}, so a [{}] query without [fusion] stays in classic mode on this node",
                name,
                HybridQueryBuilder.NAME,
                e
            );
            return null;
        }
    }
}
