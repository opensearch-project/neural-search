/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocs;

public interface HybridSearchCollector extends Collector {
    List<? extends TopDocs> topDocs();

    int getTotalHits();

    float getMaxScore();
}
