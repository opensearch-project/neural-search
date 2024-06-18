/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

/*
  SimpleFieldCollector collects the sorted results at the shard level for every individual query.
  It collects the list of TopFieldDocs.
 */
public class SimpleFieldCollector extends HybridTopFieldDocSortCollector {
    final Sort sort;
    final int numHits;

    public SimpleFieldCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, Sort sort) {
        super(numHits, hitsThresholdChecker);
        this.sort = sort;
        this.numHits = numHits;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;

        return new HybridTopDocSortLeafCollector(sort, null) {
            @Override
            public void collect(int doc) throws IOException {
                if (Objects.isNull(compoundQueryScorer)) {
                    throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                }
                float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                initializePriorityQueuesWithComparators(context, subScoresByQuery.length);
                incrementTotalHitCount();
                for (int i = 0; i < subScoresByQuery.length; i++) {
                    float score = subScoresByQuery[i];
                    // if score is 0.0 there is no hits for that sub-query
                    if (score == 0) {
                        continue;
                    }
                    collectedHits[i]++;
                    maxScore = Math.max(score, maxScore);
                    if (queueFull[i]) {
                        if (thresholdCheck(doc, i)) {
                            return;
                        }
                        collectCompetitiveHit(doc, i);
                    } else {
                        collectHit(doc, collectedHits[i], i, score);
                    }

                }
            }
        };
    }

    public List<TopFieldDocs> topDocs() {
        return super.topDocs(compoundScores, sort);
    }
}
