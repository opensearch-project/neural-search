/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

/*
  SimpleFieldCollector collects the sorted results at the shard level for every individual query.
  It collects the list of TopFieldDocs.
 */
public final class SimpleFieldCollector extends HybridTopFieldDocSortCollector {

    public SimpleFieldCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, Sort sort) {
        super(numHits, hitsThresholdChecker, sort, null);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;

        return new HybridTopDocSortLeafCollector() {
            @Override
            public void collect(int doc) throws IOException {
                if (Objects.isNull(compoundQueryScorer)) {
                    throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                }
                float[] subScoresByQuery = compoundQueryScorer.getSubQueryScores();
                initializePriorityQueuesWithComparators(context, subScoresByQuery.length);
                incrementTotalHitCount();
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }
                    maxScore = Math.max(score, maxScore);
                    if (queueFull[i]) {
                        if (thresholdCheck(doc, i)) {
                            return;
                        }
                        collectCompetitiveHit(doc, i);
                    } else {
                        collectedHits[i]++;
                        collectHit(doc, collectedHits[i], i, score);
                    }
                }
            }
        };
    }
}
