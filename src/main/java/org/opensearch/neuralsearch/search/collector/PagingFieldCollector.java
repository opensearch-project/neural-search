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

/**
 * PagingFieldCollector collects the sorted results at the shard level for every individual query
 * as per search_after criteria applied in the search request.
 * It collects the list of TopFieldDocs.
 */
public final class PagingFieldCollector extends HybridTopFieldDocSortCollector {
    private final FieldDoc after;

    public PagingFieldCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, Sort sort, @Nullable FieldDoc after) {
        super(numHits, hitsThresholdChecker, sort, after);
        this.after = after;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
        docBase = context.docBase;
        final int afterDoc = after.doc - docBase;
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

                    // if queueFull[i] is true then it indicates
                    // that we have found the results equal to the size sent in the search request.
                    if (queueFull[i]) {
                        // If threshold is reached then return. Default value of threshold is 10000.
                        if (thresholdCheck(doc, i)) {
                            return;
                        }
                    }

                    // Lets understand the below logic with example
                    // Consider there are 30 results without applying `search_after`
                    // and out of 30, 10 are the results user is seeking after applying `search_after`
                    // Therefore when those 10 results are collected the resultsFoundOnPreviousPage.
                    // the search_after parameter to retrieve the next page of hits using a set of sort values from the previous page.
                    // https://opensearch.org/docs/latest/search-plugins/searching-data/paginate/#the-search_after-parameter
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

            /**
             * It compares reverseMultiplier with the topValue in the comparator to determine
             * if the document should be included based on its position relative to the previous top value.
             * @param subQueryNumber
             * @param doc
             * @return
             * @throws IOException
             */
            private boolean checkIfSearchAfterResultsAreFound(int subQueryNumber, int doc) throws IOException {
                final int topComparison = reverseMul * comparators[subQueryNumber].compareTop(doc);
                if (topComparison > 0 || (topComparison == 0 && doc <= afterDoc)) {
                    // Already collected on a previous page
                    return true;
                }
                return false;
            }
        };
    }
}
