/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

//public record DocIdAtQueryPhase(Integer docId, SearchShardTarget searchShardTarget) {
public record DocIdAtQueryPhase(int docId, SearchShard searchShard) {
}
