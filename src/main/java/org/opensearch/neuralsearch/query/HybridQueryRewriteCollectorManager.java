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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link HybridQueryRewriteCollectorManager} is responsible for creating {@link HybridQueryExecutorCollector}
 * instances. Useful to create {@link HybridQueryExecutorCollector} instances that rewrites {@link Query} into primitive
 * {@link Query} using {@link IndexSearcher}
 */
@RequiredArgsConstructor
public final class HybridQueryRewriteCollectorManager implements HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector> {

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
        List<Query> rewrittenQueries = new ArrayList<>();
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            Query key = collector.getResult().getKey();
            rewrittenQueries.add(key);
        }
        return rewrittenQueries;
    }

    /**
     * Returns true if any of the {@link Query} from collector were actually rewritten.
     * @param collectors
     * @return at least one query is rewritten by any of the collectors
     */
    public Boolean anyQueryRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        // return true if at least one query is rewritten
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            if (collector.getResult().getValue()) {
                return true;
            }
        }
        return false;
    }

}
