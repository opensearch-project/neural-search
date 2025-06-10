/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocs;

/**
 * Common interface class for Hybrid search collectors
 */
public interface HybridSearchCollector extends Collector {
    /**
     * @return List of topDocs which contains topDocs of individual subqueries.
     */
    List<? extends TopDocs> topDocs() throws IOException;

    /**
     * @return count of total hits per shard
     */
    int getTotalHits();

    /**
     * @return maxScore found on a shard
     */
    float getMaxScore();
}
