/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;

/**
 * Abstracts normalization of scores in query search results.
 */
public interface ScoreNormalizationTechnique {

    /**
     * Performs score normalization based on input normalization technique. Mutates input object by updating normalized scores.
     * //@param queryTopDocs original query results from multiple shards and multiple sub-queries
     */
    void normalize(final NormalizeScoresDTO normalizeScoresDTO);

    // void normalize(final List<CompoundTopDocs> queryTopDocs, Map<String, Object> rrfParams);
}
