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

import java.util.List;
import java.util.stream.Collectors;

/**
 * HybridQueryScoreSupplierCollectorManager is responsible for creating {@link HybridQueryExecutorCollector} instances.
 * Useful to create {@link HybridQueryExecutorCollector} instances that build {@link ScorerSupplier} from
 * given weight.
 */
@RequiredArgsConstructor
public class HybridQueryScoreSupplierCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>, ScorerSupplier> {

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
     * mergeScoreSuppliers will build list of scoreSupplier from given collections.
     * This method should be called after HybridQueryExecutorCollector's collect method is called.
     * @param collectors List of collectors which is used to perform collection in parallel
     * @return list of {@link ScorerSupplier}
     */
    public List<ScorerSupplier> mergeScoreSuppliers(List<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> collectors) {
        List<ScorerSupplier> scorerSuppliers = collectors.stream()
            .map(HybridQueryExecutorCollector::getResult)
            .collect(Collectors.toList());
        return scorerSuppliers;
    }
}
