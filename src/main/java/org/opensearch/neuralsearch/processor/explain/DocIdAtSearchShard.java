/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import lombok.Value;
import org.opensearch.neuralsearch.processor.SearchShard;

/**
 * DTO class to store docId and search shard for a query.
 * Used in {@link org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow} to normalize scores across shards.
 */
@Value
public class DocIdAtSearchShard {
    int docId;
    SearchShard searchShard;
}
