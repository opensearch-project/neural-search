/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.dto.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

/**
 * Abstracts normalization of scores in query search results.
 *
 * <p>Extends {@link ExplainableTechnique} for the same reason the combination side does: a technique is describable by
 * construction, at no cost to an implementer since both of that interface's methods are {@code default}, and the explain
 * path reads {@code describe()} directly rather than casting.
 */
public interface ScoreNormalizationTechnique extends ExplainableTechnique {

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO is a data transfer object that contains queryTopDocs
     * original query results from multiple shards and multiple sub-queries, ScoreNormalizationTechnique,
     * and nullable rankConstant that is only used in RRF technique
     */
    void normalize(final NormalizeScoresDTO normalizeScoresDTO);

    /**
     * Returns the name of the normalization technique.
     */
    String techniqueName();
}
