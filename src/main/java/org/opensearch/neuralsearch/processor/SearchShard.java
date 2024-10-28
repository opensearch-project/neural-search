/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.search.SearchShardTarget;

public record SearchShard(String index, int shardId, String nodeId) {

    public static SearchShard create(SearchShardTarget searchShardTarget) {
        return new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId());
    }
}
