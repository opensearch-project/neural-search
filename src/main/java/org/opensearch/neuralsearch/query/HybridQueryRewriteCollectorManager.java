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

/**
 * {@link HybridQueryRewriteCollectorManager} is responsible for creating {@link HybridQueryExecutorCollector}
 * instances. Useful to create {@link HybridQueryExecutorCollector} instances that rewrites {@link Query} into primitive
 * {@link Query} using {@link IndexSearcher}
 */
@RequiredArgsConstructor
public class HybridQueryRewriteCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector, Map.Entry<Query, Boolean>> {

    private @NonNull IndexSearcher searcher;

    /**
     * Returns new {@link HybridQueryExecutorCollector} to facilitate parallel execution
     * @return HybridQueryExecutorCollector instance
     */
    @Override
    public HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> newCollector() {
        return HybridQueryExecutorCollector.newCollector(searcher);
    }

    /**
     * Returns list of {@link Query} that were rewritten by collectors
     * @param collectors
     * @return
     */
    public List<Query> getRewriteQueries(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        return collectors.stream().map(collector -> collector.getResult().getKey()).collect(Collectors.toList());
    }

    /**
     * Returns true if any of the {@link Query} from collector were actually rewritten.
     * @param collectors
     * @return at least one query is rewritten by any of the collectors
     */
    public Boolean anyQueryRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        // return true if at least one query is rewritten
        return collectors.stream().anyMatch(collector -> collector.getResult().getValue());
    }

}
