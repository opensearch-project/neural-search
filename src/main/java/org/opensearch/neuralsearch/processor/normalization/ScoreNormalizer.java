/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;

public class ScoreNormalizer {

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
        if (canQueryResultsBeNormalized(queryTopDocs)) {
            scoreNormalizationTechnique.normalize(normalizeScoresDTO);
        }
    }

    private boolean canQueryResultsBeNormalized(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream().filter(Objects::nonNull).anyMatch(topDocs -> topDocs.getTopDocs().size() > 0);
    }
}
