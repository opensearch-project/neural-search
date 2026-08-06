/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.fusion.CoordinatorScoreFusion;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.SearchPipelineService;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Coordinator-side machinery for the resolver (fused) mode: fan the sub-query legs out as a parallel {@code MultiSearch},
 * then fuse the leg hits into the standard query the {@code hybrid} query self-erases into ({@link HybridFusionQuery},
 * or {@code match_none} when nothing fused). All methods are static and take the {@link SearchRequest} /
 * {@link MultiSearchResponse} explicitly so the class holds no state.
 *
 * <p>Fusion arithmetic is NOT reimplemented here — it delegates to {@link CoordinatorScoreFusion}, the shared core that
 * classic hybrid also calls, so fused-mode relevance matches classic for the same hit set. Current scope: {@code min_max}
 * normalization + {@code arithmetic_mean} combination (the caller rejects other techniques at rewrite for now).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
final class HybridFusionOrchestrator {

    private static final ScoreCombinationFactory SCORE_COMBINATION_FACTORY = new ScoreCombinationFactory();

    /**
     * Build the leg MultiSearch: one standalone search per sub-query, each reduced to the global top-{@code windowSize}.
     * Id-only (no {@code _source}); totals disabled (the Tail supplies the full-match-set count when needed).
     *
     * <p>Each leg is pinned to the no-op search pipeline ({@code _none}). Otherwise a leg — a plain {@link SearchRequest}
     * with no explicit pipeline — would inherit the index's {@code index.search.default_pipeline} and re-run its
     * request/response processors once per leg (redundant, and incorrect for processors like {@code rerank} that expect
     * request context absent from an id-only leg). The outer fused request still carries the pipeline, so top-level
     * processors run exactly once.
     */
    static MultiSearchRequest buildLegMultiSearch(SearchRequest request, List<QueryBuilder> legs, int windowSize) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (QueryBuilder leg : legs) {
            SearchSourceBuilder legSource = new SearchSourceBuilder().query(leg)
                .size(windowSize)
                .from(0)
                .fetchSource(false)
                .trackTotalHits(false);
            multiSearchRequest.add(
                new SearchRequest(request.indices()).indicesOptions(request.indicesOptions())
                    .source(legSource)
                    .pipeline(SearchPipelineService.NOOP_PIPELINE_ID)
            );
        }
        return multiSearchRequest;
    }

    /**
     * Fuse the leg results into the standard query the fused-mode hybrid self-erases into — a {@link HybridFusionQuery}
     * (Top + conditional Tail), or a {@link MatchNoneQueryBuilder} when nothing fused. Pure: returns the query and
     * mutates nothing.
     *
     * <p>The Tail (non-scoring {@code bool{should: legs}} surfacing the full match set) is included only when the request
     * needs it (aggregations / explain / profile / highlight / leg inner_hits / totals beyond the window) and this
     * marker is the whole query. A nested fused query is always Top-only, so an enclosing filter intersects the fused
     * window at the query phase (fuse-then-filter).
     */
    static QueryBuilder buildFusedQuery(
        SearchSourceBuilder source,
        MultiSearchResponse multiSearchResponse,
        List<QueryBuilder> legs,
        FusionSpec fusion,
        int windowSize,
        boolean topLevel
    ) {
        MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        SearchHit[][] legHits = groupLegHits(items, legs.size());
        RankedDocs ranked = computeRankedDocs(legHits, fusion, windowSize);
        if (ranked.ids().length == 0) {
            return new MatchNoneQueryBuilder();
        }
        boolean topOnly;
        if (topLevel == false) {
            topOnly = true; // nested: enclosing filter intersects at the query phase
        } else if (needsExecutionTail(source) || legsHaveInnerHits(legs)) {
            topOnly = false; // aggregations / explain / profile / highlight / leg inner_hits need the legs IN the query
        } else if (wantsTotalsBeyondWindow(source, ranked.ids().length)) {
            topOnly = false; // keep the Tail for an accurate index-wide count
        } else {
            topOnly = true; // track_total_hits:false -> plain top-K, no Tail
        }
        List<QueryBuilder> tail = topOnly ? List.of() : survivingLegQueries(legs, legHits);
        return new HybridFusionQuery(ranked.ids(), ranked.scores(), tail);
    }

    /**
     * Reduce the raw MultiSearch items into a per-leg array of hits (one item per leg). Graceful per-leg failure: a
     * failed sub-search sets its slot to null and is skipped by fusion; only when ALL legs failed do we throw.
     */
    private static SearchHit[][] groupLegHits(MultiSearchResponse.Item[] items, int legCount) {
        if (items.length != legCount) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "[hybrid] expected %d leg sub-search responses but got %d", legCount, items.length)
            );
        }
        SearchHit[][] legHits = new SearchHit[legCount][];
        int survivingLegs = 0;
        for (int leg = 0; leg < legCount; leg++) {
            MultiSearchResponse.Item item = items[leg];
            if (item.isFailure()) {
                log.warn("[hybrid] fused-mode sub-query {} dropped: {}", leg, item.getFailureMessage());
                legHits[leg] = null;
            } else {
                legHits[leg] = item.getResponse().getHits().getHits();
                survivingLegs++;
            }
        }
        if (survivingLegs == 0) {
            MultiSearchResponse.Item firstFailure = firstFailure(items);
            throw new IllegalStateException(
                "[hybrid] all fused-mode sub-queries failed"
                    + (Objects.isNull(firstFailure) ? "" : ": " + firstFailure.getFailureMessage()),
                Objects.isNull(firstFailure) ? null : firstFailure.getFailure()
            );
        }
        return legHits;
    }

    private static MultiSearchResponse.Item firstFailure(MultiSearchResponse.Item[] items) {
        for (MultiSearchResponse.Item item : items) {
            if (item.isFailure()) {
                return item;
            }
        }
        return null;
    }

    /**
     * Fuse via the shared {@link CoordinatorScoreFusion} core (min_max + arithmetic_mean), then rank by fused score and
     * cut to the window. Converts the coordinator's {@code SearchHit[][]} view into the {@code _id}-keyed per-leg maps
     * the shared core consumes; a dropped (null) leg contributes an empty map.
     */
    private static RankedDocs computeRankedDocs(SearchHit[][] legHits, FusionSpec fusion, int windowSize) {
        List<Map<String, Float>> legRawScores = new ArrayList<>(legHits.length);
        for (SearchHit[] hits : legHits) {
            Map<String, Float> byId = new LinkedHashMap<>();
            if (Objects.nonNull(hits)) {
                for (SearchHit hit : hits) {
                    byId.put(hit.getId(), hit.getScore());
                }
            }
            legRawScores.add(byId);
        }
        ScoreCombinationTechnique combination = SCORE_COMBINATION_FACTORY.createCombination(
            fusion.combinationTechnique(),
            weightsParams(fusion.weights())
        );
        Map<String, Float> combined = CoordinatorScoreFusion.fuseMinMax(legRawScores, combination);
        return toRankedDocs(combined, windowSize);
    }

    private static Map<String, Object> weightsParams(float[] weights) {
        if (Objects.isNull(weights) || weights.length == 0) {
            return Map.of();
        }
        List<Double> weightsList = new ArrayList<>(weights.length);
        for (float weight : weights) {
            weightsList.add((double) weight);
        }
        return Map.of(ScoreCombinationUtil.PARAM_NAME_WEIGHTS, weightsList);
    }

    private static RankedDocs toRankedDocs(Map<String, Float> scoresById, int windowSize) {
        List<Map.Entry<String, Float>> ranked = new ArrayList<>(scoresById.entrySet());
        ranked.sort(Comparator.<Map.Entry<String, Float>>comparingDouble(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        if (ranked.size() > windowSize) {
            ranked = ranked.subList(0, windowSize);
        }
        String[] ids = new String[ranked.size()];
        float[] scores = new float[ranked.size()];
        for (int i = 0; i < ranked.size(); i++) {
            ids[i] = ranked.get(i).getKey();
            scores[i] = ranked.get(i).getValue();
        }
        return new RankedDocs(ids, scores);
    }

    /** The sub-query legs restricted to those that survived (non-null hits slot); used for the Tail so a failed leg is
     *  not re-executed in the self-erased query (graceful degradation). */
    private static List<QueryBuilder> survivingLegQueries(List<QueryBuilder> legs, SearchHit[][] legHits) {
        List<QueryBuilder> surviving = new ArrayList<>(legs.size());
        for (int legIndex = 0; legIndex < legs.size(); legIndex++) {
            if (legIndex >= legHits.length || Objects.nonNull(legHits[legIndex])) {
                QueryBuilder leg = legs.get(legIndex);
                // A kNN/neural leg's match set IS its returned top-k — re-running it in the Tail would walk the HNSW
                // graph again purely to count. Materialize such legs as their already-retrieved ids instead.
                if (isMaterializableLeg(leg) && legIndex < legHits.length && Objects.nonNull(legHits[legIndex])) {
                    IdsQueryBuilder ids = new IdsQueryBuilder();
                    for (SearchHit hit : legHits[legIndex]) {
                        ids.addIds(hit.getId());
                    }
                    surviving.add(ids);
                } else {
                    surviving.add(leg);
                }
            }
        }
        return surviving;
    }

    /** Legs whose Lucene match set is their own top-k (re-running them in the Tail = a redundant ANN pass). */
    private static boolean isMaterializableLeg(QueryBuilder leg) {
        String name = leg.getWriteableName();
        return "knn".equals(name) || "neural".equals(name) || "neural_knn".equals(name);
    }

    private static boolean needsExecutionTail(SearchSourceBuilder source) {
        return Objects.nonNull(source)
            && (Objects.nonNull(source.aggregations())
                || Boolean.TRUE.equals(source.explain())
                || source.profile()
                || Objects.nonNull(source.highlighter()));
    }

    private static boolean legsHaveInnerHits(List<QueryBuilder> legs) {
        Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
        for (QueryBuilder leg : legs) {
            InnerHitContextBuilder.extractInnerHits(leg, innerHits);
        }
        return innerHits.isEmpty() == false;
    }

    private static boolean wantsTotalsBeyondWindow(SearchSourceBuilder source, int numRankedDocs) {
        if (Objects.isNull(source)) {
            return true;
        }
        Integer trackTotalHitsUpTo = source.trackTotalHitsUpTo();
        return Objects.isNull(trackTotalHitsUpTo) || trackTotalHitsUpTo > numRankedDocs;
    }

    private record RankedDocs(String[] ids, float[] scores) {
    }
}
