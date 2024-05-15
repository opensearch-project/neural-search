/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollector;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollectorManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class HybridQueryRewriteCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector, Map.Entry<Query, Boolean>> {

    private @NonNull IndexSearcher searcher;

    @Override
    public HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> newCollector() {
        return HybridQueryExecutorCollector.newCollector(searcher);
    }

    public List<Query> getRewriteQueries(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        return collectors.stream().map(collector -> collector.getResult().getKey()).collect(Collectors.toList());
    }

    public Boolean anyQueryRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        // return true if at least one query is rewritten
        return collectors.stream().anyMatch(collector -> collector.getResult().getValue());
    }

}
