/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.HasAggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.completion.CompletionSuggestion;

/**
 * The node by which a rescored fused hit's first pass is found again in its explanation.
 *
 * <p>Core explains a rescore by nesting what it rescored — {@code Rescorer#explain}'s {@code sourceExplanation}, the
 * first pass — inside layers of its own, and by computing every value above it from that node's value. For a fused
 * hybrid the first pass is the self-erased query, which the coordinator replaces with the fused breakdown. Rather than
 * recognise where core put it — the layers' descriptions and layout are core's to change — the fused rescore guard
 * wraps the first pass, on the shard and before any rescorer sees it, in a node this plugin owns ({@link #mark}), and the
 * coordinator finds that node by its own shape — its description over a single child — wherever core nested it
 * ({@link #replace}). The wrapper carries the first pass's own value, so every number core computes around it is
 * unchanged.
 *
 * <p>The wrapper is a hand-off between the guard and the coordinator, not something a user should read. Wherever no
 * fused breakdown takes its place — a document fusion did not rank, an inner hit, a {@code top_hits} bucket, a
 * completion suggestion's hit, all of which the same rescorers explain — {@link #stripAll} removes it again, which
 * restores exactly the tree core built.
 */
public final class FusedFirstPassMarker {

    /**
     * Plugin-owned, and worded so that it stays true should a node ever reach a response unreplaced.
     *
     * <p>This string and the shape {@link #isMarker} requires (a match carrying it over exactly one child) cross the
     * boundary between the shards that write the marker and the coordinator that replaces it, which can run different
     * versions while a cluster upgrades. Do not change either in place: a new form has to be accepted by the coordinator
     * for at least a release before shards start writing it.
     */
    public static final String DESCRIPTION = "first pass of the fused hybrid query:";

    private FusedFirstPassMarker() {}

    /**
     * The first pass wrapped in the marker, or {@code firstPass} itself when there is nothing to mark: a {@code null}, or
     * a first pass that did not match, which core does not compute a value from.
     */
    public static Explanation mark(final Explanation firstPass) {
        if (Objects.isNull(firstPass) || firstPass.isMatch() == false) {
            return firstPass;
        }
        return Explanation.match(firstPass.getValue(), DESCRIPTION, firstPass);
    }

    /**
     * The tree with every marker replaced by what {@code replacement} returns for it, and every node on the way rebuilt
     * with its own value, description and other children. Returns {@code node} itself when the tree holds no marker, and
     * {@code null} when {@code replacement} returns {@code null} for any marker. Pure: the input is never mutated.
     */
    static Explanation replace(final Explanation node, final UnaryOperator<Explanation> replacement) {
        if (isMarker(node)) {
            return replacement.apply(node);
        }
        Explanation[] details = node.getDetails();
        Explanation[] rebuilt = null;
        for (int i = 0; i < details.length; i++) {
            Explanation child = replace(details[i], replacement);
            if (Objects.isNull(child)) {
                return null;
            }
            if (child != details[i]) {
                if (Objects.isNull(rebuilt)) {
                    rebuilt = details.clone();
                }
                rebuilt[i] = child;
            }
        }
        if (Objects.isNull(rebuilt)) {
            return node;
        }
        return node.isMatch()
            ? Explanation.match(node.getValue(), node.getDescription(), rebuilt)
            : Explanation.noMatch(node.getDescription(), rebuilt);
    }

    /** The tree with every marker replaced by the first pass it wraps, i.e. exactly what core built. */
    static Explanation strip(final Explanation node) {
        return replace(node, marker -> marker.getDetails()[0]);
    }

    /**
     * Strip the marker from every explanation in the response that the guard's {@code explain} can have reached: the
     * hits, their inner hits, the hits of every {@code top_hits} aggregation at any depth, and the hits of completion
     * suggestion options, which the fetch phase explains along with the page. Mutates the hits in place, as
     * {@link FusedExplanationMerger} does.
     */
    public static void stripAll(final SearchResponse response) {
        if (Objects.isNull(response)) {
            return;
        }
        stripHits(response.getHits());
        stripAggregations(response.getAggregations());
        stripSuggestions(response.getSuggest());
    }

    private static boolean isMarker(final Explanation node) {
        return node.isMatch() && DESCRIPTION.equals(node.getDescription()) && node.getDetails().length == 1;
    }

    private static void stripHits(final SearchHits hits) {
        if (Objects.isNull(hits) || Objects.isNull(hits.getHits())) {
            return;
        }
        for (SearchHit hit : hits.getHits()) {
            stripHit(hit);
        }
    }

    private static void stripHit(final SearchHit hit) {
        Explanation explanation = hit.getExplanation();
        if (Objects.nonNull(explanation)) {
            Explanation stripped = strip(explanation);
            if (stripped != explanation) {
                hit.explanation(stripped);
            }
        }
        Map<String, SearchHits> innerHits = hit.getInnerHits();
        if (Objects.nonNull(innerHits)) {
            innerHits.values().forEach(FusedFirstPassMarker::stripHits);
        }
    }

    private static void stripSuggestions(final Suggest suggest) {
        if (Objects.isNull(suggest)) {
            return;
        }
        for (CompletionSuggestion suggestion : suggest.filter(CompletionSuggestion.class)) {
            for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
                if (Objects.nonNull(option.getHit())) {
                    stripHit(option.getHit());
                }
            }
        }
    }

    /**
     * The same walk {@code FusedRescoreScoreNormalizer} takes to reach {@code top_hits} buckets. Package-private so the
     * nesting branches can be driven directly: the bucket aggregations that reach them have package-private constructors.
     */
    static void stripAggregations(final Aggregations aggregations) {
        if (Objects.isNull(aggregations)) {
            return;
        }
        for (Aggregation aggregation : aggregations.asList()) {
            if (aggregation instanceof TopHits topHits) {
                stripHits(topHits.getHits());
            } else if (aggregation instanceof MultiBucketsAggregation multiBucket) {
                for (MultiBucketsAggregation.Bucket bucket : multiBucket.getBuckets()) {
                    stripAggregations(bucket.getAggregations());
                }
            } else if (aggregation instanceof HasAggregations hasAggregations) {
                stripAggregations(hasAggregations.getAggregations());
            }
        }
    }
}
