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

@NoArgsConstructor
public class HybridQueryScoresCollectionManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>>, Float> {

    @Override
    public HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>> newCollector() {
        return HybridQueryExecutorCollector.newCollector(null);
    }

    public void updateScores(final List<HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>>> collectors, final float[] scores) {
        for (HybridQueryExecutorCollector<?, Map.Entry<Integer, Float>> collector : collectors) {
            scores[collector.getResult().getKey()] = collector.getResult().getValue();
        }
    }
}
