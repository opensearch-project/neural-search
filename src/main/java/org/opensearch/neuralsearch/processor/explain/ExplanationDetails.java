/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

/**
 * DTO class to store value and description for explain details.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 * @param value
 * @param description
 */
public record ExplanationDetails(int docId, float value, String description) {
    public ExplanationDetails(float value, String description) {
        this(-1, value, description);
    }
}
