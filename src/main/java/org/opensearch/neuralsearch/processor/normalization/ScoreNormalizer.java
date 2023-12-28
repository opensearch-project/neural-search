/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.CompoundTopDocs;

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
}
