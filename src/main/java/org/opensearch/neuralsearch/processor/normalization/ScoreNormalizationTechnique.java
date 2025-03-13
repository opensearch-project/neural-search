/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;

/**
 * Abstracts normalization of scores in query search results.
 */
public interface ScoreNormalizationTechnique {

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO is a data transfer object that contains queryTopDocs
     * original query results from multiple shards and multiple sub-queries, ScoreNormalizationTechnique,
     * and nullable rankConstant that is only used in RRF technique
     */
    void normalize(final NormalizeScoresDTO normalizeScoresDTO);

    void validateCombinationTechnique(final ScoreCombinationTechnique combinationTechnique) throws IllegalArgumentException;
}
