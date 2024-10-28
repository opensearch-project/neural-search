/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

/**
 * Data class to store value and description for explain details.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 * @param value
 * @param description
 */
public record ExplainDetails(float value, String description, int docId) {

    public ExplainDetails(float value, String description) {
        this(value, description, -1);
    }
}
