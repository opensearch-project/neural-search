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
import java.util.Optional;

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
     * Returns list of {@link Query} that were rewritten by collectors. If collector doesn't
     * have any result, null will be inserted to the result.
     * This method must be called after collection is finished on all provided collectors.
     * @param collectors list of collectors
     * @return list of {@link Query} that was rewritten by corresponding collector from input.
     */
    public List<Query> getRewriteQueries(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        List<Query> rewrittenQueries = new ArrayList<>();
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            if (collector.getResult().isPresent()) {
                rewrittenQueries.add(collector.getResult().get().getKey());
            } else {
                // if for some reason collector didn't have result, we will add null to its
                // position in the result.
                rewrittenQueries.add(null);
            }
        }
        return rewrittenQueries;
    }

    /**
     * Returns true if any of the {@link Query} from collector were actually rewritten.
     * If any of the given collector doesn't have result, it will be ignored as if that
     * instance did not exist. This method must be called after collection is finished
     * on all provided collectors.
     * @param collectors List of collectors to check any of their query was rewritten during
     *                   collect step.
     * @return at least one query is rewritten by any of the collectors
     */
    public boolean anyQueryRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        // return true if at least one query is rewritten
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            final Optional<Map.Entry<Query, Boolean>> result = collector.getResult();
            if (result.isPresent() && result.get().getValue()) {
                return true;
            }
        }
        return false;
    }
}
