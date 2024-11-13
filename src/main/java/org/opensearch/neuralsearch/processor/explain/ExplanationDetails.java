/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * DTO class to store value and description for explain details.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 */
@Value
@AllArgsConstructor
public class ExplanationDetails {
    int docId;
    List<Pair<Float, String>> scoreDetails;

    public ExplanationDetails(List<Pair<Float, String>> scoreDetails) {
        this(-1, scoreDetails);
    }
}
