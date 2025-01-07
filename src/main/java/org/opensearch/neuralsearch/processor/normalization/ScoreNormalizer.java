/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

public class ScoreNormalizer {

    /**
     * Performs score normalization based on input normalization technique. Mutates input object by updating normalized scores.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     * @param scoreNormalizationTechnique exact normalization technique that should be applied
     */
    public void normalizeScores(final List<CompoundTopDocs> queryTopDocs, final ScoreNormalizationTechnique scoreNormalizationTechnique) {
        if (canQueryResultsBeNormalized(queryTopDocs)) {
            scoreNormalizationTechnique.normalize(queryTopDocs);
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
