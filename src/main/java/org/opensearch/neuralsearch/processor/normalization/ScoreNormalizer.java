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
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.HybridScoreRegistry;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.isSortEnabled;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.isExplainEnabled;

public class ScoreNormalizer {
    @Getter
    @Setter
    private static SearchPhaseContext searchPhaseContext;

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO used as data transfer object to pass in queryTopDocs, original query results
     * from multiple shards and multiple sub-queries, scoreNormalizationTechnique exact normalization technique
     * that should be applied, and nullable rankConstant that is only used in RRF technique
     */
    public void normalizeScores(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        final ScoreNormalizationTechnique scoreNormalizationTechnique = normalizeScoresDTO.getNormalizationTechnique();
        final SearchPhaseContext searchPhaseContext = normalizeScoresDTO.getSearchPhaseContext();
        if (canQueryResultsBeNormalized(queryTopDocs)) {

            Map<String, float[]> hybridizationScores = scoreNormalizationTechnique.normalize(normalizeScoresDTO);

            boolean isExplainEnabled = isExplainEnabled(searchPhaseContext);
            boolean isSortEnabled = isSortEnabled(searchPhaseContext);

            if (isExplainEnabled == false && isSortEnabled == false) {
                // Store in registry
                setSearchPhaseContext(searchPhaseContext);
                HybridScoreRegistry.store(searchPhaseContext, hybridizationScores);

                // clean up later via context.addReleasable()
                searchPhaseContext.addReleasable(() -> HybridScoreRegistry.remove(searchPhaseContext));
            }
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
