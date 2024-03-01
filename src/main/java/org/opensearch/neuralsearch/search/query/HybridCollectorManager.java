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
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.DocValueFormat;
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

@RequiredArgsConstructor
public abstract class HybridCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final boolean isSingleShard;
    private final int trackTotalHitsUpTo;
    private final SortAndFormats sortAndFormats;

    public static CollectorManager createHybridCollectorManager(final SearchContext searchContext) throws IOException {
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final int totalNumDocs = Math.max(0, reader.numDocs());
        boolean isSingleShard = searchContext.numberOfShards() == 1;
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        int trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();

        return searchContext.shouldUseConcurrentSearch()
            ? new HybridCollectorConcurrentSearchManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                isSingleShard,
                trackTotalHitsUpTo,
                searchContext.sort()
            )
            : new HybridCollectorNonConcurrentManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                isSingleShard,
                trackTotalHitsUpTo,
                searchContext.sort()
            );
    }

    @Override
    abstract public Collector newCollector();

    Collector getCollector() {
        Collector hybridcollector = new HybridTopScoreDocCollector(numHits, hitsThresholdChecker);
        return hybridcollector;
    }

    @Override
    public ReduceableSearchResult reduce(Collection<Collector> collectors) {
        final List<HybridTopScoreDocCollector> hybridTopScoreDocCollectors = new ArrayList<>();

        for (final Collector collector : collectors) {
            if (collector instanceof MultiCollectorWrapper) {
                for (final Collector sub : (((MultiCollectorWrapper) collector).getCollectors())) {
                    if (sub instanceof HybridTopScoreDocCollector) {
                        hybridTopScoreDocCollectors.add((HybridTopScoreDocCollector) sub);
                    }
                }
            } else if (collector instanceof HybridTopScoreDocCollector) {
                hybridTopScoreDocCollectors.add((HybridTopScoreDocCollector) collector);
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

    static class HybridCollectorNonConcurrentManager extends HybridCollectorManager {
        Collector maxScoreCollector;

        public HybridCollectorNonConcurrentManager(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            boolean isSingleShard,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats
        ) {
            super(numHits, hitsThresholdChecker, isSingleShard, trackTotalHitsUpTo, sortAndFormats);
        }

        @Override
        public Collector newCollector() {
            if (Objects.isNull(maxScoreCollector)) {
                maxScoreCollector = getCollector();
                return maxScoreCollector;
            } else {
                Collector toReturnCollector = maxScoreCollector;
                maxScoreCollector = null;
                return toReturnCollector;
            }
        }
    }

    static class HybridCollectorConcurrentSearchManager extends HybridCollectorManager {

        public HybridCollectorConcurrentSearchManager(
            int numHits,
            HitsThresholdChecker hitsThresholdChecker,
            boolean isSingleShard,
            int trackTotalHitsUpTo,
            SortAndFormats sortAndFormats
        ) {
            super(numHits, hitsThresholdChecker, isSingleShard, trackTotalHitsUpTo, sortAndFormats);
        }

        @Override
        public Collector newCollector() {
            return getCollector();
        }
    }
}
