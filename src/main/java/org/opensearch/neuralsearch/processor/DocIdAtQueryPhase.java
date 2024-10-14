/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

/**
 * Data class to store docId and search shard for a query.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 * @param docId
 * @param searchShard
 */
public record DocIdAtQueryPhase(int docId, SearchShard searchShard) {
}
