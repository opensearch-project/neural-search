/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Objects;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.query.FusedWindowGuardRescorerBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.HasAggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

/**
 * Turns the fused rescore guard's internal band values back into the scores a user should see.
 *
 * <p>{@code FusedWindowGuardRescorer} separates the documents fusion ranked from the documents it did not by putting the
 * latter in a band below every ranked score — it has to be a score, because the score is the only channel a shard has to
 * the coordinator's merge, and {@code sort} is refused alongside {@code rescore} so there is no sort-value channel. The
 * band is two floats wide and both of them are meaningless to a user: an unranked document reports
 * {@code -3.4028235E38} ({@code UNRANKED_SCORE}) and a ranked document whose own arithmetic saturated reports the float
 * immediately above it ({@code RANKED_FLOOR}).
 *
 * <p><b>One decoding rule covers both:</b> report what the document would have scored with no rescore in the request at
 * all. That is exactly {@code 0.0} for an unranked document, which matches only the non-scoring Tail, and
 * {@code RANKED_FLOOR_NORMALIZED} for a saturated ranked one, which is the floor every fused score already sits at or
 * above. Decoding only the unranked half would leave a ranked hit printing {@code -3.4028233E38} while the unranked hits
 * it outranks print {@code 0.0} — right order, and a page whose scores descend into nonsense.
 *
 * <p>Scores are mutated in place, the way {@code FusedExplanationMerger} mutates explanations: {@code SearchHit} has a
 * public score setter and both the response and {@code InternalTopHits} pass their {@code SearchHits} through by
 * reference. {@code SearchHits.maxScore} is {@code final}, so a page whose top hit was demoted needs the hits rebuilt.
 *
 * <p><b>{@code top_hits} buckets are decoded too, and are not a corner case.</b> Core applies the request's own rescore
 * contexts to every bucket's {@code TopDocs} and writes the resulting score straight onto the bucket's hit, so the guard
 * demotes inside a bucket exactly as it does on the page — and an aggregation is precisely the shape that forces the
 * Tail open, so a bucket asking for more hits than fusion ranked shows unranked documents with no unusual weights
 * anywhere in the request.
 *
 * <p><b>The one residual, and it cannot be fixed from a plugin:</b> a bucket's own {@code max_score}. Core keeps it in
 * the {@code final} {@code SearchHits.maxScore} of an {@code InternalTopHits} whose {@code TopDocsAndMaxScore} accessor
 * is package-private, so the aggregation cannot be faithfully rebuilt from outside {@code org.opensearch.search}. It
 * surfaces only for a bucket that contains no ranked document at all — the bucket's top hit is otherwise a ranked one —
 * and the bucket's hits themselves are correct.
 */
final class FusedRescoreScoreNormalizer {

    private FusedRescoreScoreNormalizer() {}

    /**
     * @param response the response as the search phases built it
     * @param sentinel the score the guard demoted unranked documents to
     * @return the response with every band value decoded, or {@code response} itself when its {@code maxScore} did not
     *         have to change — a fused request whose rescore never had to demote anything pays only the walk
     */
    static SearchResponse normalize(final SearchResponse response, final float sentinel) {
        if (Objects.isNull(response) || Objects.isNull(response.getInternalResponse())) {
            return response;
        }
        decodeAggregations(response.getInternalResponse().aggregations(), sentinel);
        SearchHits hits = response.getInternalResponse().hits();
        if (Objects.isNull(hits) || Objects.isNull(hits.getHits())) {
            return response;
        }
        decodeHitScores(hits.getHits(), sentinel);
        // Decoded through the same rule as the hits rather than assumed to be the unranked sentinel: the top hit of a
        // page can be a ranked document that saturated, and reporting 0.0 for it would put the page's maximum below its
        // own ranked hits. This is the only field here that cannot be corrected in place.
        float maxScore = decode(hits.getMaxScore(), sentinel);
        if (Float.compare(maxScore, hits.getMaxScore()) == 0) {
            return response;
        }
        SearchHits corrected = new SearchHits(
            hits.getHits(),
            hits.getTotalHits(),
            maxScore,
            hits.getSortFields(),
            hits.getCollapseField(),
            hits.getCollapseValues()
        );
        return FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), corrected);
    }

    /**
     * The band value a document is sitting on, mapped to the score it would have had with no rescore. Anything outside
     * the band is the user's own rescore arithmetic and is returned untouched, however negative it is.
     */
    private static float decode(final float score, final float sentinel) {
        if (Float.compare(score, sentinel) == 0) {
            return 0.0f;
        }
        if (Float.compare(score, FusedWindowGuardRescorerBuilder.RANKED_FLOOR) == 0) {
            return FusedWindowGuardRescorerBuilder.RANKED_FLOOR_NORMALIZED;
        }
        return score;
    }

    private static void decodeHitScores(final SearchHit[] hits, final float sentinel) {
        for (SearchHit hit : hits) {
            float decoded = decode(hit.getScore(), sentinel);
            if (Float.compare(decoded, hit.getScore()) != 0) {
                hit.score(decoded);
            }
        }
    }

    /**
     * Walk the aggregation tree for {@code top_hits}, the second place core applies the request's rescorers. Read-only
     * on the tree itself — only the {@code SearchHit} scores are written — so nothing here depends on an aggregation
     * being rebuildable.
     *
     * <p>Package-private rather than private so the nesting branches can be driven directly: the single-bucket and
     * multi-bucket aggregations that reach them have package-private constructors in core, and the shapes that do
     * exercise them end to end live in an integration test, which earns no coverage in {@code check}.
     */
    static void decodeAggregations(final Aggregations aggregations, final float sentinel) {
        if (Objects.isNull(aggregations)) {
            return;
        }
        for (Aggregation aggregation : aggregations.asList()) {
            if (aggregation instanceof TopHits topHits) {
                SearchHits bucketHits = topHits.getHits();
                if (Objects.nonNull(bucketHits) && Objects.nonNull(bucketHits.getHits())) {
                    decodeHitScores(bucketHits.getHits(), sentinel);
                }
            } else if (aggregation instanceof MultiBucketsAggregation multiBucket) {
                for (MultiBucketsAggregation.Bucket bucket : multiBucket.getBuckets()) {
                    decodeAggregations(bucket.getAggregations(), sentinel);
                }
            } else if (aggregation instanceof HasAggregations hasAggregations) {
                // Every single-bucket aggregation — filter, nested, global, reverse_nested — reaches its children here.
                decodeAggregations(hasAggregations.getAggregations(), sentinel);
            }
        }
    }
}
