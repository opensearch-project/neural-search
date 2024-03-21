/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;

/**
 * Collector manager based on HybridTopScoreDocCollector that allows users to parallelize counting the number of hits.
 * In most cases it will be wrapped in MultiCollectorManager.
 */
@RequiredArgsConstructor
public abstract class HybridCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final boolean isSingleShard;
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
        boolean isSingleShard = searchContext.numberOfShards() == 1;
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
                isSingleShard,
                trackTotalHitsUpTo,
                searchContext.sort(),
                filteringWeight
            )
            : new HybridCollectorNonConcurrentManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                isSingleShard,
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

        if (!hybridTopScoreDocCollectors.isEmpty()) {
            HybridTopScoreDocCollector hybridTopScoreDocCollector = hybridTopScoreDocCollectors.stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("cannot collect results of hybrid search query"));
            List<TopDocs> topDocs = hybridTopScoreDocCollector.topDocs();
            TopDocs newTopDocs = getNewTopDocs(getTotalHits(this.trackTotalHitsUpTo, topDocs, isSingleShard), topDocs);
            float maxScore = getMaxScore(topDocs);
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(newTopDocs, maxScore);
            return (QuerySearchResult result) -> { result.topDocs(topDocsAndMaxScore, getSortValueFormats(sortAndFormats)); };
        }
        throw new IllegalStateException("cannot collect results of hybrid search query, there are no proper score collectors");
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

    private TotalHits getTotalHits(int trackTotalHitsUpTo, final List<TopDocs> topDocs, final boolean isSingleShard) {
        final TotalHits.Relation relation = trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
        if (topDocs == null || topDocs.isEmpty()) {
            return new TotalHits(0, relation);
        }

        List<ScoreDoc[]> scoreDocs = topDocs.stream()
            .map(topdDoc -> topdDoc.scoreDocs)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        Set<Integer> uniqueDocIds = new HashSet<>();
        for (ScoreDoc[] scoreDocsArray : scoreDocs) {
            uniqueDocIds.addAll(Arrays.stream(scoreDocsArray).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toList()));
        }
        long maxTotalHits = uniqueDocIds.size();

        return new TotalHits(maxTotalHits, relation);
    }

    private float getMaxScore(final List<TopDocs> topDocs) {
        if (topDocs.isEmpty()) {
            return 0.0f;
        } else {
            return topDocs.stream()
                .map(docs -> docs.scoreDocs.length == 0 ? new ScoreDoc(-1, 0.0f) : docs.scoreDocs[0])
                .map(scoreDoc -> scoreDoc.score)
                .max(Float::compare)
                .get();
        }
    }

    private DocValueFormat[] getSortValueFormats(final SortAndFormats sortAndFormats) {
        return sortAndFormats == null ? null : sortAndFormats.formats;
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
            boolean isSingleShard,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats,
            Weight filteringWeight
        ) {
            super(numHits, hitsThresholdChecker, isSingleShard, trackTotalHitsUpTo, sortAndFormats, filteringWeight);
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
            boolean isSingleShard,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats,
            Weight filteringWeight
        ) {
            super(numHits, hitsThresholdChecker, isSingleShard, trackTotalHitsUpTo, sortAndFormats, filteringWeight);
        }
    }
}
