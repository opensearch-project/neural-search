/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.neuralsearch.query.HybridQueryScorer;

@Log4j2
public abstract class HybridSortedTopDocCollector implements Collector {

    private abstract class HybridSortedTopDocLeafCollector implements LeafCollector {
        final LeafFieldComparator comparator;
        final int reverseMul;
        // oolean collectedAllCompetitiveHits = false;
        HybridQueryScorer compoundQueryScorer;

        private HybridSortedTopDocLeafCollector(FieldValueHitQueue<FieldValueHitQueue.Entry> queue, Sort sort, LeafReaderContext context)
            throws IOException {
            LeafFieldComparator[] comparators = queue.getComparators(context);
            int[] reverseMuls = queue.getReverseMul();
            if (comparators.length == 1) {
                this.reverseMul = reverseMuls[0];
                this.comparator = comparators[0];
            } else {
                this.reverseMul = 1;
                this.comparator = new MultiLeafFieldComparator(comparators, reverseMuls);
            }

        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (scorer instanceof HybridQueryScorer) {
                log.debug("passed scorer is of type HybridQueryScorer, saving it for collecting documents and scores");
                compoundQueryScorer = (HybridQueryScorer) scorer;
            } else {
                compoundQueryScorer = getHybridQueryScorer(scorer);
                if (Objects.isNull(compoundQueryScorer)) {
                    log.error(
                        String.format(Locale.ROOT, "cannot find scorer of type HybridQueryScorer in a hierarchy of scorer %s", scorer)
                    );
                }
            }
            comparator.setScorer(compoundQueryScorer);
        }

        private HybridQueryScorer getHybridQueryScorer(final Scorable scorer) throws IOException {
            if (scorer == null) {
                return null;
            }
            if (scorer instanceof HybridQueryScorer) {
                return (HybridQueryScorer) scorer;
            }
            for (Scorable.ChildScorable childScorable : scorer.getChildren()) {
                HybridQueryScorer hybridQueryScorer = getHybridQueryScorer(childScorable.child);
                if (Objects.nonNull(hybridQueryScorer)) {
                    log.debug(
                        String.format(
                            Locale.ROOT,
                            "found hybrid query scorer, it's child of scorer %s",
                            childScorable.child.getClass().getSimpleName()
                        )
                    );
                    return hybridQueryScorer;
                }
            }
            return null;
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            return comparator.competitiveIterator();
        }
    }

    private static final TopFieldDocs EMPTY_TOPDOCS = new TopFieldDocs(
        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
        new ScoreDoc[0],
        new SortField[0]
    );
    int docBase;
    private final HitsThresholdChecker hitsThresholdChecker;
    private TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;
    private static int[] totalHits;
    // private final int numHits;
    @Getter
    private static PriorityQueue<ScoreDoc>[] compoundScores;
    // the current local minimum competitive score already propagated to the underlying scorer
    float minCompetitiveScore;
    final Sort sort;

    public HybridSortedTopDocCollector(
        int numHits,
        HitsThresholdChecker hitsThresholdChecker,
        FieldValueHitQueue<FieldValueHitQueue.Entry> queue,
        Sort sort
    ) {
        this.hitsThresholdChecker = hitsThresholdChecker;
        // this.numHits = numHits;
        this.sort = sort;
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    public static class SimpleFieldCollector extends HybridSortedTopDocCollector {
        final Sort sort;
        final FieldValueHitQueue<FieldValueHitQueue.Entry> queue;
        final int numHits;

        public SimpleFieldCollector(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            FieldValueHitQueue<FieldValueHitQueue.Entry> queue,
            Sort sort
        ) {
            super(numHits, hitsThresholdChecker, queue, sort);
            this.sort = sort;
            this.queue = queue;
            this.numHits = numHits;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            minCompetitiveScore = 0f;
            docBase = context.docBase;

            return new HybridSortedTopDocLeafCollector(queue, sort, context) {
                @Override
                public void collect(int doc) throws IOException {
                    if (Objects.isNull(compoundQueryScorer)) {
                        throw new IllegalArgumentException("scorers are null for all sub-queries in hybrid query");
                    }
                    float[] subScoresByQuery = compoundQueryScorer.hybridScores();
                    // iterate over results for each query
                    if (compoundScores == null) {
                        compoundScores = new PriorityQueue[subScoresByQuery.length];
                        for (int i = 0; i < subScoresByQuery.length; i++) {
                            compoundScores[i] = new HitQueue(numHits, true);
                        }
                        totalHits = new int[subScoresByQuery.length];
                    }
                    for (int i = 0; i < subScoresByQuery.length; i++) {
                        float score = subScoresByQuery[i];
                        // if score is 0.0 there is no hits for that sub-query
                        if (score == 0) {
                            continue;
                        }
                        totalHits[i]++;
                        PriorityQueue<ScoreDoc> pq = compoundScores[i];
                        ScoreDoc topDoc = pq.top();
                        topDoc.doc = doc + docBase;
                        topDoc.score = score;
                        pq.updateTop();
                    }
                }
            };
        }

    }

    public List<TopDocs> topDocs() {
        if (compoundScores == null) {
            return new ArrayList<>();
        }
        final List<TopDocs> topFieldDocs = IntStream.range(0, compoundScores.length)
            .mapToObj(
                i -> topDocsPerQuery(0, Math.min(totalHits[i], compoundScores[i].size()), compoundScores[i], totalHits[i], sort.getSort())
            )
            .collect(Collectors.toList());
        return topFieldDocs;
    }

    private TopDocs topDocsPerQuery(int start, int howMany, PriorityQueue<ScoreDoc> pq, int totalHits, SortField[] sortFields) {
        if (howMany < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Number of hits requested must be greater than 0 but value was %d", howMany)
            );
        }

        if (start < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected value of starting position is between 0 and %d, got %d", howMany, start)
            );
        }

        if (start >= howMany || howMany == 0) {
            return EMPTY_TOPDOCS;
        }

        int size = howMany - start;
        ScoreDoc[] results = new ScoreDoc[size];
        // pq's pop() returns the 'least' element in the queue, therefore need
        // to discard the first ones, until we reach the requested range.
        for (int i = pq.size() - start - size; i > 0; i--) {
            pq.pop();
        }

        // Get the requested results from pq.
        populateResults(results, size, pq);

        return new TopFieldDocs(new TotalHits(totalHits, totalHitsRelation), results, sortFields);
    }

    protected void populateResults(ScoreDoc[] results, int howMany, PriorityQueue<ScoreDoc> pq) {
        for (int i = howMany - 1; i >= 0 && pq.size() > 0; i--) {
            // adding to array if index is within [0..array_length - 1]
            if (i < results.length) {
                results[i] = pq.pop();
            }
        }
    }
}
