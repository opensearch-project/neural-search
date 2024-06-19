/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;

/*
  PagingFieldCollector collects the sorted results at the shard level for every individual query
  as per search_after criteria applied in the search request.
  It collects the list of TopFieldDocs.
 */
public final class PagingFieldCollector extends HybridTopFieldDocSortCollector {

    private final Sort sort;
    private final FieldDoc after;

    public PagingFieldCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, Sort sort, @Nullable FieldDoc after) {
        super(numHits, hitsThresholdChecker, sort);
        this.sort = sort;
        this.after = after;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;
        final int afterDoc = after.doc - docBase;
        return new HybridTopDocSortLeafCollector(sort, after) {
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

                    if (queueFull[i]) {
                        if (thresholdCheck(doc, i)) {
                            return;
                        }
                    }

                    // logic for search_after
                    boolean resultsFoundOnPreviousPage = checkIfSearchAfterResultsAreFound(i, doc);
                    if (resultsFoundOnPreviousPage) {
                        return;
                    }
                    maxScore = Math.max(score, maxScore);
                    if (queueFull[i]) {
                        collectCompetitiveHit(doc, i);
                    } else {
                        collectedHits[i]++;
                        collectHit(doc, collectedHits[i], i, score);
                    }

                }

            }

            private boolean checkIfSearchAfterResultsAreFound(int subQueryNumber, int doc) throws IOException {
                final int topCmp = reverseMul * comparators[subQueryNumber].compareTop(doc);
                if (topCmp > 0 || (topCmp == 0 && doc <= afterDoc)) {
                    // Already collected on a previous page
                    return true;
                }
                return false;
            }
        };
    }
}
