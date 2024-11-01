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
public record ExplainDetails(int docId, float value, String description) {
    public ExplainDetails(float value, String description) {
        this(-1, value, description);
    }
}
