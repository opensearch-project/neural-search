/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.FilteredCollector;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.MultiCollectorWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.search.TotalHits.Relation;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryScoreDocElement;

/**
 * Collector manager based on HybridTopScoreDocCollector that allows users to parallelize counting the number of hits.
 * In most cases it will be wrapped in MultiCollectorManager.
 */
@RequiredArgsConstructor
public abstract class HybridCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private static final int MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC = 3;
    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final int trackTotalHitsUpTo;
    private final SortAndFormats sortAndFormats;
    @Nullable
    private final Weight filterWeight;
    private static final float boost_factor = 1f;

    /**
     * Create new instance of HybridCollectorManager depending on the concurrent search beeing enabled or disabled.
     * @param searchContext
     * @return
     * @throws IOException
     */
    public static CollectorManager createHybridCollectorManager(final SearchContext searchContext) throws IOException {
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final int totalNumDocs = Math.max(0, reader.numDocs());
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        int trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();

        Weight filteringWeight = null;
        // Check for post filter to create weight for filter query and later use that weight in the search workflow
        if (Objects.nonNull(searchContext.parsedPostFilter()) && Objects.nonNull(searchContext.parsedPostFilter().query())) {
            Query filterQuery = searchContext.parsedPostFilter().query();
            ContextIndexSearcher searcher = searchContext.searcher();
            // ScoreMode COMPLETE_NO_SCORES will be passed as post_filter does not contribute in scoring. COMPLETE_NO_SCORES means it is not
            // a scoring clause
            // Boost factor 1f is taken because if boost is multiplicative of 1 then it means "no boost"
            // Previously this code in OpenSearch looked like
            // https://github.com/opensearch-project/OpenSearch/commit/36a5cf8f35e5cbaa1ff857b5a5db8c02edc1a187
            filteringWeight = searcher.createWeight(searcher.rewrite(filterQuery), ScoreMode.COMPLETE_NO_SCORES, boost_factor);
        }

        return searchContext.shouldUseConcurrentSearch()
            ? new HybridCollectorConcurrentSearchManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                trackTotalHitsUpTo,
                searchContext.sort(),
                filteringWeight
            )
            : new HybridCollectorNonConcurrentManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                trackTotalHitsUpTo,
                searchContext.sort(),
                filteringWeight
            );
    }

    @Override
    public Collector newCollector() {
        Collector hybridcollector = new HybridTopScoreDocCollector(numHits, hitsThresholdChecker);
        // Check if filterWeight is present. If it is present then return wrap Hybrid collector object underneath the FilteredCollector
        // object and return it.
        return Objects.nonNull(filterWeight) ? new FilteredCollector(hybridcollector, filterWeight) : hybridcollector;
    }

    /**
     * Reduce the results from hybrid scores collector into a format specific for hybrid search query:
     * - start
     * - sub-query-delimiter
     * - scores
     * - stop
     * Ignore other collectors if they are present in the context
     * @param collectors collection of collectors after they has been executed and collected documents and scores
     * @return search results that can be reduced be the caller
     */
    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) {
        final List<HybridTopScoreDocCollector> hybridTopScoreDocCollectors = getHybridScoreDocCollectors(collectors);
        if (hybridTopScoreDocCollectors.isEmpty()) {
            throw new IllegalStateException("cannot collect results of hybrid search query, there are no proper score collectors");
        }

        List<ReduceableSearchResult> results = new ArrayList<>();
        DocValueFormat[] docValueFormats = getSortValueFormats(sortAndFormats);
        for (HybridTopScoreDocCollector hybridTopScoreDocCollector : hybridTopScoreDocCollectors) {
            List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();
            TopDocs newTopDocs = getNewTopDocs(
                getTotalHits(this.trackTotalHitsUpTo, topDocs, hybridTopScoreDocCollector.getTotalHits()),
                topDocs
            );
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(newTopDocs, hybridTopScoreDocCollector.getMaxScore());

            results.add((QuerySearchResult result) -> reduceCollectorResults(result, topDocsAndMaxScore, docValueFormats, newTopDocs));
        }
        return reduceSearchResults(results);
    }

    private List<HybridTopScoreDocCollector> getHybridScoreDocCollectors(Collection<Collector> collectors) {
        final List<HybridTopScoreDocCollector> hybridTopScoreDocCollectors = new ArrayList<>();
        // check if collector for hybrid query scores is part of this search context. It can be wrapped into MultiCollectorWrapper
        // in case multiple collector managers are registered. We use hybrid scores collector to format scores into
        // format specific for hybrid search query: start, sub-query-delimiter, scores, stop
        for (final Collector collector : collectors) {
            if (collector instanceof MultiCollectorWrapper) {
                for (final Collector sub : (((MultiCollectorWrapper) collector).getCollectors())) {
                    if (sub instanceof HybridTopScoreDocCollector) {
                        hybridTopScoreDocCollectors.add((HybridTopScoreDocCollector) sub);
                    }
                }
            } else if (collector instanceof HybridTopScoreDocCollector) {
                hybridTopScoreDocCollectors.add((HybridTopScoreDocCollector) collector);
            } else if (collector instanceof FilteredCollector
                && ((FilteredCollector) collector).getCollector() instanceof HybridTopScoreDocCollector) {
                    hybridTopScoreDocCollectors.add((HybridTopScoreDocCollector) ((FilteredCollector) collector).getCollector());
                }
        }
        return hybridTopScoreDocCollectors;
    }

    private TopDocs getNewTopDocs(final TotalHits totalHits, final List<TopDocs> topDocs) {
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        if (Objects.nonNull(topDocs)) {
            // for a single shard case we need to do score processing at coordinator level.
            // this is workaround for current core behaviour, for single shard fetch phase is executed
            // right after query phase and processors are called after actual fetch is done
            // find any valid doc Id, or set it to -1 if there is not a single match
            int delimiterDocId = topDocs.stream()
                .filter(Objects::nonNull)
                .filter(topDoc -> Objects.nonNull(topDoc.scoreDocs))
                .map(topDoc -> topDoc.scoreDocs)
                .filter(scoreDoc -> scoreDoc.length > 0)
                .map(scoreDoc -> scoreDoc[0].doc)
                .findFirst()
                .orElse(-1);
            if (delimiterDocId == -1) {
                return new TopDocs(totalHits, scoreDocs);
            }
            // format scores using following template:
            // doc_id | magic_number_1
            // doc_id | magic_number_2
            // ...
            // doc_id | magic_number_2
            // ...
            // doc_id | magic_number_2
            // ...
            // doc_id | magic_number_1
            List<ScoreDoc> result = new ArrayList<>();
            result.add(createStartStopElementForHybridSearchResults(delimiterDocId));
            for (TopDocs topDoc : topDocs) {
                if (Objects.isNull(topDoc) || Objects.isNull(topDoc.scoreDocs)) {
                    result.add(createDelimiterElementForHybridSearchResults(delimiterDocId));
                    continue;
                }
                result.add(createDelimiterElementForHybridSearchResults(delimiterDocId));
                result.addAll(Arrays.asList(topDoc.scoreDocs));
            }
            result.add(createStartStopElementForHybridSearchResults(delimiterDocId));
            scoreDocs = result.stream().map(doc -> new ScoreDoc(doc.doc, doc.score, doc.shardIndex)).toArray(ScoreDoc[]::new);
        }
        return new TopDocs(totalHits, scoreDocs);
    }

    private TotalHits getTotalHits(int trackTotalHitsUpTo, final List<TopDocs> topDocs, final long maxTotalHits) {
        final Relation relation = trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED
            ? Relation.GREATER_THAN_OR_EQUAL_TO
            : Relation.EQUAL_TO;
        if (topDocs == null || topDocs.isEmpty()) {
            return new TotalHits(0, relation);
        }

        return new TotalHits(maxTotalHits, relation);
    }

    private DocValueFormat[] getSortValueFormats(final SortAndFormats sortAndFormats) {
        return sortAndFormats == null ? null : sortAndFormats.formats;
    }

    private void reduceCollectorResults(
        QuerySearchResult result,
        TopDocsAndMaxScore topDocsAndMaxScore,
        DocValueFormat[] docValueFormats,
        TopDocs newTopDocs
    ) {
        // this is case of first collector, query result object doesn't have any top docs set, so we can
        // just set new top docs without merge
        if (result.hasConsumedTopDocs()) {
            result.topDocs(topDocsAndMaxScore, docValueFormats);
            return;
        }
        // in this case top docs are already present in result, and we need to merge next result object with what we have.
        // if collector doesn't have any hits we can just skip it and save some cycles by not doing merge
        if (newTopDocs.totalHits.value == 0) {
            return;
        }
        // we need to do actual merge because query result and current collector both have some score hits
        TopDocsAndMaxScore originalTotalDocsAndHits = result.topDocs();
        result.topDocs(mergeTopDocsAndMaxScores(originalTotalDocsAndHits, topDocsAndMaxScore), docValueFormats);
    }

    /**
     * For collection of search results, return a single one that has results from all individual result objects.
     * @param results collection of search results
     * @return single search result that represents all results as one object
     */
    private ReduceableSearchResult reduceSearchResults(List<ReduceableSearchResult> results) {
        return (result) -> {
            for (ReduceableSearchResult r : results) {
                // call reduce for results of each single collector, this will update top docs in query result
                r.reduce(result);
            }
        };
    }

    @VisibleForTesting
    protected TopDocsAndMaxScore mergeTopDocsAndMaxScores(TopDocsAndMaxScore source, TopDocsAndMaxScore newTopDocs) {
        if (Objects.isNull(newTopDocs) || Objects.isNull(newTopDocs.topDocs) || newTopDocs.topDocs.totalHits.value == 0) {
            return source;
        }
        // we need to merge hits per individual sub-query
        // format of results in both new and source TopDocs is following
        // doc_id | magic_number_1
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_2
        // ...
        // doc_id | magic_number_1
        ScoreDoc[] sourceScoreDocs = source.topDocs.scoreDocs;
        ScoreDoc[] newScoreDocs = newTopDocs.topDocs.scoreDocs;

        List<ScoreDoc> mergedScoreDocs = mergedScoreDocs(sourceScoreDocs, newScoreDocs, Comparator.comparing((scoreDoc) -> scoreDoc.score));
        TotalHits mergedTotalHits = getMergedTotalHits(source, newTopDocs);
        TopDocsAndMaxScore result = new TopDocsAndMaxScore(
            new TopDocs(mergedTotalHits, mergedScoreDocs.toArray(new ScoreDoc[0])),
            Math.max(source.maxScore, newTopDocs.maxScore)
        );
        return result;
    }

    /**
     * Merge two score docs objects, result ScoreDocs[] object will have all hits per sub-query from both original objects.
     * Logic is based on assumption that hits of every sub-query are sorted by score.
     * Method returns new object and doesn't mutate original ScoreDocs arrays.
     * @param sourceScoreDocs original score docs from query result
     * @param newScoreDocs new score docs that we need to merge into existing scores
     * @return merged array of ScoreDocs objects
     */
    private List<ScoreDoc> mergedScoreDocs(
        final ScoreDoc[] sourceScoreDocs,
        final ScoreDoc[] newScoreDocs,
        final Comparator<ScoreDoc> scoreDocComparator
    ) {
        if (Objects.requireNonNull(sourceScoreDocs).length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC
            || Objects.requireNonNull(newScoreDocs).length < MIN_NUMBER_OF_ELEMENTS_IN_SCORE_DOC) {
            throw new IllegalArgumentException("cannot merge top docs because it does not have enough elements");
        }
        // we overshoot and preallocate more than we need - length of both top docs combined.
        // we will take only portion of the array at the end
        List<ScoreDoc> mergedScoreDocs = new ArrayList<>(sourceScoreDocs.length + newScoreDocs.length);
        int sourcePointer = 0;
        // mark beginning of hybrid query results by start element
        mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
        sourcePointer++;
        // new pointer is set to 1 as we don't care about it start-stop element
        int newPointer = 1;

        while (sourcePointer < sourceScoreDocs.length - 1 && newPointer < newScoreDocs.length - 1) {
            // every iteration is for results of one sub-query
            mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
            sourcePointer++;
            newPointer++;
            // simplest case when both arrays have results for sub-query
            while (sourcePointer < sourceScoreDocs.length
                && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])
                && newPointer < newScoreDocs.length
                && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                if (scoreDocComparator.compare(sourceScoreDocs[sourcePointer], newScoreDocs[newPointer]) >= 0) {
                    mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                    sourcePointer++;
                } else {
                    mergedScoreDocs.add(newScoreDocs[newPointer]);
                    newPointer++;
                }
            }
            // at least one object got exhausted at this point, now merge all elements from object that's left
            while (sourcePointer < sourceScoreDocs.length && isHybridQueryScoreDocElement(sourceScoreDocs[sourcePointer])) {
                mergedScoreDocs.add(sourceScoreDocs[sourcePointer]);
                sourcePointer++;
            }
            while (newPointer < newScoreDocs.length && isHybridQueryScoreDocElement(newScoreDocs[newPointer])) {
                mergedScoreDocs.add(newScoreDocs[newPointer]);
                newPointer++;
            }
        }
        // mark end of hybrid query results by end element
        mergedScoreDocs.add(sourceScoreDocs[sourceScoreDocs.length - 1]);
        return mergedScoreDocs;
    }

    private TotalHits getMergedTotalHits(TopDocsAndMaxScore source, TopDocsAndMaxScore newTopDocs) {
        // merged value is a lower bound - if both are equal_to than merged will also be equal_to,
        // otherwise assign greater_than_or_equal
        Relation mergedHitsRelation = source.topDocs.totalHits.relation == Relation.GREATER_THAN_OR_EQUAL_TO
            || newTopDocs.topDocs.totalHits.relation == Relation.GREATER_THAN_OR_EQUAL_TO
                ? Relation.GREATER_THAN_OR_EQUAL_TO
                : Relation.EQUAL_TO;
        return new TotalHits(source.topDocs.totalHits.value + newTopDocs.topDocs.totalHits.value, mergedHitsRelation);
    }

    /**
     * Implementation of the HybridCollector that reuses instance of collector on each even call. This allows caller to
     * use saved state of collector
     */
    static class HybridCollectorNonConcurrentManager extends HybridCollectorManager {
        private final Collector scoreCollector;

        public HybridCollectorNonConcurrentManager(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats,
            Weight filteringWeight
        ) {
            super(numHits, hitsThresholdChecker, trackTotalHitsUpTo, sortAndFormats, filteringWeight);
            scoreCollector = Objects.requireNonNull(super.newCollector(), "collector for hybrid query cannot be null");
        }

        @Override
        public Collector newCollector() {
            return scoreCollector;
        }

        @Override
        public ReduceableSearchResult reduce(Collection<Collector> collectors) {
            assert collectors.isEmpty() : "reduce on HybridCollectorNonConcurrentManager called with non-empty collectors";
            return super.reduce(List.of(scoreCollector));
        }
    }

    /**
     * Implementation of the HybridCollector that doesn't save collector's state and return new instance of every
     * call of newCollector
     */
    static class HybridCollectorConcurrentSearchManager extends HybridCollectorManager {

        public HybridCollectorConcurrentSearchManager(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats,
            Weight filteringWeight
        ) {
            super(numHits, hitsThresholdChecker, trackTotalHitsUpTo, sortAndFormats, filteringWeight);
        }
    }
}
