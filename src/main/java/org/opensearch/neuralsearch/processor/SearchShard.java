/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.opensearch.search.SearchShardTarget;

/**
 * DTO class to store index, shardId and nodeId for a search shard.
 */
@Value
@AllArgsConstructor
public class SearchShard {
    String index;
    int shardId;
    String nodeId;

    /**
     * Create SearchShard from SearchShardTarget
     * @param searchShardTarget
     * @return SearchShard
     */
    public static SearchShard createSearchShard(final SearchShardTarget searchShardTarget) {
        return new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId());
    }
}
