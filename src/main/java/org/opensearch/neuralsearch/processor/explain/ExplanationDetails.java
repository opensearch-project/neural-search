/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * DTO class to store value and description for explain details.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 * @param docId iterator based id of the document
 * @param scoreDetails list of score details for the document, each Pair object contains score and description of the score
 */
public record ExplanationDetails(int docId, List<Pair<Float, String>> scoreDetails) {

    public ExplanationDetails(List<Pair<Float, String>> scoreDetails) {
        this(-1, scoreDetails);
    }
}
