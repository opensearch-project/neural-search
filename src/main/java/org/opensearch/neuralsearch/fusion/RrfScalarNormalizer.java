/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;

/**
 * {@code rrf} implementation of {@link ScalarNormalizer}: reciprocal rank fusion expressed as a per-leg normalization.
 * Ranks the leg's scores, then replaces each score with its rank score {@code 1/(rank_constant + rank + 1)} via the
 * shared {@link RRFScoreNormalizer} — the same arithmetic {@link RRFNormalizationTechnique} applies shard-side, so the
 * rank score for a given rank is identical on both paths.
 *
 * <p>RRF fits {@link ScalarNormalizer} because that contract is a whole-leg transform: rank is derived from the leg's
 * full value set, exactly as {@code min_max} derives min/max from it. Nothing about it is a special case for
 * {@link CoordinatorScoreFusion}, which normalizes then combines without knowing which technique it holds. It is the
 * only technique carrying a parameter, which is why {@link ScalarNormalizers} maps names to factories rather than
 * singletons.
 *
 * <p>Unlike the score-based techniques, raw score <em>magnitude</em> is irrelevant here — only the order within a leg
 * matters — so a leg whose scores are all equal yields the same rank scores as one spread across orders of magnitude,
 * assuming the same key order.
 *
 * <p><b>Tie order differs from classic, deliberately.</b> Classic ranks tied scores by Lucene {@code docId} then shard
 * id; neither exists on the coordinator, so ties here break on the ascending fusion key. Classic's order is not a
 * stable contract — {@code docId} is reassigned by segment merges and reindexing, {@code shardId} by re-sharding, so
 * the same tied documents can change relative order with no change to data or query — whereas the key is independent
 * of physical layout. Tied documents may therefore receive different rank scores on the two paths; untied documents
 * may not.
 */
public final class RrfScalarNormalizer implements ScalarNormalizer {

    private final int rankConstant;

    /**
     * @param rankConstant RRF rank constant, already validated (see {@link RRFScoreNormalizer#validateRankConstant})
     */
    RrfScalarNormalizer(final int rankConstant) {
        this.rankConstant = rankConstant;
    }

    @Override
    public Map<String, Float> normalizeLeg(final Map<String, Float> legRawScores) {
        final Map<String, Integer> ranks = RRFScoreNormalizer.assignRanksByScoreDescending(legRawScores);
        final Map<String, Float> normalized = new LinkedHashMap<>();
        // Iterate the input rather than the ranks so the returned map keeps the leg's key order, matching what the
        // score-based normalizers hand back; every input key is ranked, so this loses nothing.
        for (String key : legRawScores.keySet()) {
            normalized.put(key, RRFScoreNormalizer.scoreForRank(ranks.get(key), rankConstant));
        }
        return normalized;
    }

    @Override
    public String techniqueName() {
        return RRFNormalizationTechnique.TECHNIQUE_NAME;
    }

    /**
     * Overridden because rrf is the one technique here that carries a parameter, and the name alone would describe a
     * normalization the request did not ask for: two queries differing only in {@code rank_constant} score differently and
     * would otherwise explain identically. Rendered through the shared
     * {@link RRFScoreNormalizer#describeWithRankConstant} so this reads exactly as {@link RRFNormalizationTechnique}'s
     * {@code describe()} does for the same rank constant.
     */
    @Override
    public String describe() {
        return RRFScoreNormalizer.describeWithRankConstant(rankConstant);
    }
}
