/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.NoArgsConstructor;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollector;
import org.opensearch.neuralsearch.executors.HybridQueryExecutorCollectorManager;

import java.util.List;
import java.util.Map;

/**
 * {@link HybridQueryScoresCollectionManager} is responsible for creating {@link HybridQueryExecutorCollector} instances.
 * Useful to create {@link HybridQueryExecutorCollector} instances that calls score method on individual
 * scorer
 */
@NoArgsConstructor
public class HybridQueryScoresCollectionManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>>, Float> {

    /**
     * Returns new {@link HybridQueryExecutorCollector} instance to facilitate parallel execution
     * by individual tasks
     * @return HybridQueryExecutorCollector instance
     */
    @Override
    public HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>> newCollector() {
        return HybridQueryExecutorCollector.newCollector(null);
    }

    /**
     * Update scores from collectors that was previously collected from scorer.
     * Collector will provide score and index of scorer to map it back to scores array.
     * @param collectors List of scorers where we want to calculate score.
     * @param scores Float array to combine scores from available scores
     */
    public void updateScores(final List<HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>>> collectors, final float[] scores) {
        for (HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>> collector : collectors) {
            scores[collector.getResult().getKey()] = collector.getResult().getValue();
        }
    }
}
