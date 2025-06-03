/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.HybridScoreRegistry;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.search.internal.SearchContext;

public class ScoreNormalizer {
    @Getter
    @Setter
    private static SearchContext searchContext;

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO used as data transfer object to pass in queryTopDocs, original query results
     * from multiple shards and multiple sub-queries, scoreNormalizationTechnique exact normalization technique
     * that should be applied, and nullable rankConstant that is only used in RRF technique
     */
    public void normalizeScores(final NormalizeScoresDTO normalizeScoresDTO, SearchContext searchContext) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        final ScoreNormalizationTechnique scoreNormalizationTechnique = normalizeScoresDTO.getNormalizationTechnique();
        if (canQueryResultsBeNormalized(queryTopDocs)) {

            Map<Integer, float[]> hybridizationScores = scoreNormalizationTechnique.normalize(normalizeScoresDTO);
            // Store in registry

            setSearchContext(searchContext);
            HybridScoreRegistry.store(searchContext, hybridizationScores);

            // // // Optional: clean up later via context.addReleasable()
            // searchContext.addReleasable(() -> HybridScoreRegistry.remove(searchContext));
        }
    }

    private boolean canQueryResultsBeNormalized(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream().filter(Objects::nonNull).anyMatch(topDocs -> topDocs.getTopDocs().size() > 0);
    }

    /**
     * Explain normalized scores based on input normalization technique. Does not mutate input object.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     * @param queryTopDocs
     * @param scoreNormalizationTechnique
     * @return map of doc id to explanation details
     */
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(
        final List<CompoundTopDocs> queryTopDocs,
        final ExplainableTechnique scoreNormalizationTechnique
    ) {
        if (canQueryResultsBeNormalized(queryTopDocs)) {
            return scoreNormalizationTechnique.explain(queryTopDocs);
        }
        return Map.of();
    }
}
