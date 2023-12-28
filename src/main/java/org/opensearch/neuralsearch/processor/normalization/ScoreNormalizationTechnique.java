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

import org.opensearch.neuralsearch.processor.CompoundTopDocs;

/**
 * Abstracts normalization of scores in query search results.
 */
public interface ScoreNormalizationTechnique {

    /**
     * Performs score normalization based on input normalization technique. Mutates input object by updating normalized scores.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     */
    void normalize(final List<CompoundTopDocs> queryTopDocs);
}
