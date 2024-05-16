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

@RequiredArgsConstructor
public class HybridQueryScoreSupplierCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>, ScorerSupplier> {

    private @NonNull LeafReaderContext context;

    @Override
    public HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> newCollector() {
        return HybridQueryExecutorCollector.newCollector(context);
    }

    public List<ScorerSupplier> mergeScoreSuppliers(List<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> collectors) {
        List<ScorerSupplier> scorerSuppliers = collectors.stream()
            .map(HybridQueryExecutorCollector::getResult)
            .collect(Collectors.toList());
        return scorerSuppliers;
    }
}
