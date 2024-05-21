/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScorerSupplier;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollector;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollectorManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * HybridQueryScoreSupplierCollectorManager is responsible for creating {@link HybridQueryExecutorCollector} instances.
 * Useful to create {@link HybridQueryExecutorCollector} instances that build {@link ScorerSupplier} from
 * given weight.
 */
@RequiredArgsConstructor
public final class HybridQueryScoreSupplierCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> {

    private @NonNull LeafReaderContext context;

    /**
     * Creates new {@link HybridQueryExecutorCollector} instance everytime to facilitate parallel execution
     * by individual tasks
     * @return new instance of HybridQueryExecutorCollector
     */
    @Override
    public HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> newCollector() {
        return HybridQueryExecutorCollector.newCollector(context);
    }

    /**
     * mergeScoreSuppliers will build list of scoreSupplier from given list of collectors.
     * This method should be called after HybridQueryExecutorCollector's collect method is called.
     * If collectors didn't have any result, null will be added to list.
     * @param collectors List of collectors which is used to perform collection in parallel
     * @return list of {@link ScorerSupplier}
     */
    public List<ScorerSupplier> mergeScoreSuppliers(List<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> collectors) {
        List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
        for (HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> collector : collectors) {
            Optional<ScorerSupplier> result = collector.getResult();
            if (result.isPresent()) {
                scorerSuppliers.add(result.get());
            } else {
                scorerSuppliers.add(null);
            }
        }
        return scorerSuppliers;
    }
}
