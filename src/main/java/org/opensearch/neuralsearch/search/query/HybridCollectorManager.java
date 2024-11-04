/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import java.util.Locale;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.FieldDoc;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.FilteredCollector;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopFieldDocSortCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopScoreDocCollector;
import org.opensearch.neuralsearch.search.collector.SimpleFieldCollector;
import org.opensearch.neuralsearch.search.collector.PagingFieldCollector;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.MultiCollectorWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.neuralsearch.search.query.exception.HybridSearchRescoreQueryException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.lucene.search.TotalHits.Relation;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createSortFieldsForDelimiterResults;

/**
 * Collector manager based on HybridTopScoreDocCollector that allows users to parallelize counting the number of hits.
 * In most cases it will be wrapped in MultiCollectorManager.
 */
@RequiredArgsConstructor
@Log4j2
public abstract class HybridCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final int trackTotalHitsUpTo;
    private final SortAndFormats sortAndFormats;
    @Nullable
    private final Weight filterWeight;
    private static final float boostFactor = 1f;
    private final TopDocsMerger topDocsMerger;
    @Nullable
    private final FieldDoc after;
    private final SearchContext searchContext;

    /**
     * Create new instance of HybridCollectorManager depending on the concurrent search beeing enabled or disabled.
     * @param searchContext
     * @return
     * @throws IOException
     */
    public static CollectorManager createHybridCollectorManager(final SearchContext searchContext) throws IOException {
        if (searchContext.scrollContext() != null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Scroll operation is not supported in hybrid query"));
        }
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final int totalNumDocs = Math.max(0, reader.numDocs());
        int numDocs = Math.min(getSubqueryResultsRetrievalSize(searchContext), totalNumDocs);
        int trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();
        if (searchContext.sort() != null) {
            validateSortCriteria(searchContext, searchContext.trackScores());
        }

        boolean isSingleShard = searchContext.numberOfShards() == 1;
        // In case of single shard, it can happen that fetch phase might execute before normalization phase. Moreover, The pagination logic
        // lies in the fetch phase.
        // If the fetch phase gets executed before the normalization phase, then the result will be not paginated as per normalized score.
        // Therefore, to avoid it we will update from value in search context to 0. This will stop fetch phase to trim results prematurely.
        // Later in the normalization phase we will update QuerySearchResult object with the right from value, to handle the effective
        // trimming of results.
        if (isSingleShard && searchContext.from() > 0) {
            searchContext.from(0);
        }

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
            filteringWeight = searcher.createWeight(searcher.rewrite(filterQuery), ScoreMode.COMPLETE_NO_SCORES, boostFactor);
        }

        return searchContext.shouldUseConcurrentSearch()
            ? new HybridCollectorConcurrentSearchManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                trackTotalHitsUpTo,
                filteringWeight,
                searchContext
            )
            : new HybridCollectorNonConcurrentManager(
                numDocs,
                new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo())),
                trackTotalHitsUpTo,
                filteringWeight,
                searchContext
            );
    }

    @Override
    public Collector newCollector() {
        Collector hybridCollector = getHybridQueryCollector();
        // Check if filterWeight is present. If it is present then return wrap Hybrid Sort collector object underneath the FilteredCollector
        // object and return it.
        return Objects.nonNull(filterWeight) ? new FilteredCollector(hybridCollector, filterWeight) : hybridCollector;
    }

    private Collector getHybridQueryCollector() {
        if (sortAndFormats == null) {
            return new HybridTopScoreDocCollector(numHits, hitsThresholdChecker);
        } else {
            // Sorting is applied
            if (after == null) {
                return new SimpleFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort);
            } else {
                // search_after is applied
                validateSearchAfterFieldAndSortFormats();
                return new PagingFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort, after);
            }
        }
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
        final List<HybridSearchCollector> hybridSearchCollectors = getHybridSearchCollectors(collectors);
        if (hybridSearchCollectors.isEmpty()) {
            throw new IllegalStateException("cannot collect results of hybrid search query, there are no proper collectors");
        }
        return reduceSearchResults(getSearchResults(hybridSearchCollectors));
    }

    private List<ReduceableSearchResult> getSearchResults(final List<HybridSearchCollector> hybridSearchCollectors) {
        List<ReduceableSearchResult> results = new ArrayList<>();
        DocValueFormat[] docValueFormats = getSortValueFormats(sortAndFormats);
        for (HybridSearchCollector collector : hybridSearchCollectors) {
            boolean isSortEnabled = docValueFormats != null;
            TopDocsAndMaxScore topDocsAndMaxScore = getTopDocsAndAndMaxScore(collector, isSortEnabled);
            results.add((QuerySearchResult result) -> reduceCollectorResults(result, topDocsAndMaxScore, docValueFormats));
        }
        return results;
    }

    private TopDocsAndMaxScore getTopDocsAndAndMaxScore(final HybridSearchCollector hybridSearchCollector, final boolean isSortEnabled) {
        List topDocs = hybridSearchCollector.topDocs();
        if (isSortEnabled) {
            return getSortedTopDocsAndMaxScore(topDocs, hybridSearchCollector);
        }
        return getTopDocsAndMaxScore(topDocs, hybridSearchCollector);
    }

    private TopDocsAndMaxScore getSortedTopDocsAndMaxScore(List<TopFieldDocs> topDocs, HybridSearchCollector hybridSearchCollector) {
        TopDocs sortedTopDocs = getNewTopFieldDocs(
            getTotalHits(this.trackTotalHitsUpTo, topDocs, hybridSearchCollector.getTotalHits()),
            topDocs,
            sortAndFormats.sort.getSort()
        );
        return new TopDocsAndMaxScore(sortedTopDocs, hybridSearchCollector.getMaxScore());
    }

    private TopDocsAndMaxScore getTopDocsAndMaxScore(List<TopDocs> topDocs, HybridSearchCollector hybridSearchCollector) {
        if (shouldRescore()) {
            topDocs = rescore(topDocs);
        }
        float maxScore = calculateMaxScore(topDocs, hybridSearchCollector.getMaxScore());
        TopDocs finalTopDocs = getNewTopDocs(getTotalHits(this.trackTotalHitsUpTo, topDocs, hybridSearchCollector.getTotalHits()), topDocs);
        return new TopDocsAndMaxScore(finalTopDocs, maxScore);
    }

    private boolean shouldRescore() {
        List<RescoreContext> rescoreContexts = searchContext.rescore();
        return Objects.nonNull(rescoreContexts) && !rescoreContexts.isEmpty();
    }

    private List<TopDocs> rescore(List<TopDocs> topDocs) {
        List<TopDocs> rescoredTopDocs = topDocs;
        for (RescoreContext ctx : searchContext.rescore()) {
            rescoredTopDocs = rescoredTopDocs(ctx, rescoredTopDocs);
        }
        return rescoredTopDocs;
    }

    /**
     * Rescores the top documents using the provided context. The input topDocs may be modified during this process.
     */
    private List<TopDocs> rescoredTopDocs(final RescoreContext ctx, final List<TopDocs> topDocs) {
        List<TopDocs> result = new ArrayList<>(topDocs.size());
        for (TopDocs topDoc : topDocs) {
            try {
                result.add(ctx.rescorer().rescore(topDoc, searchContext.searcher(), ctx));
            } catch (IOException exception) {
                log.error("rescore failed for hybrid query in collector_manager.reduce call", exception);
                throw new HybridSearchRescoreQueryException(exception);
            }
        }
        return result;
    }

    /**
    * Calculates the maximum score from the provided TopDocs, considering rescoring.
    */
    private float calculateMaxScore(List<TopDocs> topDocsList, float initialMaxScore) {
        List<RescoreContext> rescoreContexts = searchContext.rescore();
        if (Objects.nonNull(rescoreContexts) && !rescoreContexts.isEmpty()) {
            for (TopDocs topDocs : topDocsList) {
                if (Objects.nonNull(topDocs.scoreDocs) && topDocs.scoreDocs.length > 0) {
                    // first top doc for each sub-query has the max score because top docs are sorted by score desc
                    initialMaxScore = Math.max(initialMaxScore, topDocs.scoreDocs[0].score);
                }
            }
        }
        return initialMaxScore;
    }

    private List<HybridSearchCollector> getHybridSearchCollectors(final Collection<Collector> collectors) {
        final List<HybridSearchCollector> hybridSearchCollectors = new ArrayList<>();
        for (final Collector collector : collectors) {
            if (collector instanceof MultiCollectorWrapper) {
                for (final Collector sub : (((MultiCollectorWrapper) collector).getCollectors())) {
                    if (sub instanceof HybridTopScoreDocCollector || sub instanceof HybridTopFieldDocSortCollector) {
                        hybridSearchCollectors.add((HybridSearchCollector) sub);
                    }
                }
            } else if (collector instanceof HybridTopScoreDocCollector || collector instanceof HybridTopFieldDocSortCollector) {
                hybridSearchCollectors.add((HybridSearchCollector) collector);
            } else if (collector instanceof FilteredCollector
                && (((FilteredCollector) collector).getCollector() instanceof HybridTopScoreDocCollector
                    || ((FilteredCollector) collector).getCollector() instanceof HybridTopFieldDocSortCollector)) {
                        hybridSearchCollectors.add((HybridSearchCollector) ((FilteredCollector) collector).getCollector());
                    }
        }
        return hybridSearchCollectors;
    }

    private static void validateSortCriteria(SearchContext searchContext, boolean trackScores) {
        SortField[] sortFields = searchContext.sort().sort.getSort();
        boolean hasFieldSort = false;
        boolean hasScoreSort = false;
        for (SortField sortField : sortFields) {
            SortField.Type type = sortField.getType();
            if (type.equals(SortField.Type.SCORE)) {
                hasScoreSort = true;
            } else {
                hasFieldSort = true;
            }
            if (hasScoreSort && hasFieldSort) {
                break;
            }
        }
        if (hasScoreSort && hasFieldSort) {
            throw new IllegalArgumentException(
                "_score sort criteria cannot be applied with any other criteria. Please select one sort criteria out of them."
            );
        }
        if (trackScores && hasFieldSort) {
            throw new IllegalArgumentException(
                "Hybrid search results when sorted by any field, docId or _id, track_scores must be set to false."
            );
        }
        if (trackScores && hasScoreSort) {
            throw new IllegalArgumentException("Hybrid search results are by default sorted by _score, track_scores must be set to false.");
        }
    }

    private void validateSearchAfterFieldAndSortFormats() {
        if (after.fields == null) {
            throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
        }

        if (after.fields.length != sortAndFormats.sort.getSort().length) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "after.fields has %s values but sort has %s",
                    after.fields.length,
                    sortAndFormats.sort.getSort().length
                )
            );
        }
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

    private TotalHits getTotalHits(int trackTotalHitsUpTo, final List<?> topDocs, final long maxTotalHits) {
        final Relation relation = trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED
            ? Relation.GREATER_THAN_OR_EQUAL_TO
            : Relation.EQUAL_TO;

        if (topDocs == null || topDocs.isEmpty()) {
            return new TotalHits(0, relation);
        }

        return new TotalHits(maxTotalHits, relation);
    }

    private TopDocs getNewTopFieldDocs(final TotalHits totalHits, final List<TopFieldDocs> topFieldDocs, final SortField sortFields[]) {
        if (Objects.isNull(topFieldDocs)) {
            return new TopFieldDocs(totalHits, new FieldDoc[0], sortFields);
        }

        // for a single shard case we need to do score processing at coordinator level.
        // this is workaround for current core behaviour, for single shard fetch phase is executed
        // right after query phase and processors are called after actual fetch is done
        // find any valid doc Id, or set it to -1 if there is not a single match
        int delimiterDocId = topFieldDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDoc -> Objects.nonNull(topDoc.scoreDocs))
            .map(topFieldDoc -> topFieldDoc.scoreDocs)
            .filter(scoreDoc -> scoreDoc.length > 0)
            .map(scoreDoc -> scoreDoc[0].doc)
            .findFirst()
            .orElse(-1);
        if (delimiterDocId == -1) {
            return new TopFieldDocs(totalHits, new FieldDoc[0], sortFields);
        }

        // format scores using following template:
        // consider the sort is applied for two fields.
        // consider field1 type is integer and field2 type is float.
        // doc_id | magic_number_1 | [1,1.0f]
        // doc_id | magic_number_2 | [1,1.0f]
        // ...
        // doc_id | magic_number_2 | [1,1.0f]
        // ...
        // doc_id | magic_number_2 | [1,1.0f]
        // ...
        // doc_id | magic_number_1 | [1,1.0f]
        final Object[] sortFieldsForDelimiterResults = createSortFieldsForDelimiterResults(sortFields);
        List<FieldDoc> result = new ArrayList<>();
        result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, sortFieldsForDelimiterResults));
        for (TopFieldDocs topFieldDoc : topFieldDocs) {
            if (Objects.isNull(topFieldDoc) || Objects.isNull(topFieldDoc.scoreDocs)) {
                result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, sortFieldsForDelimiterResults));
                continue;
            }

            List<FieldDoc> fieldDocsPerQuery = new ArrayList<>();
            for (ScoreDoc scoreDoc : topFieldDoc.scoreDocs) {
                fieldDocsPerQuery.add((FieldDoc) scoreDoc);
            }
            result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, sortFieldsForDelimiterResults));
            result.addAll(fieldDocsPerQuery);
        }
        result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, sortFieldsForDelimiterResults));

        FieldDoc[] fieldDocs = result.toArray(new FieldDoc[0]);

        return new TopFieldDocs(totalHits, fieldDocs, sortFields);
    }

    private DocValueFormat[] getSortValueFormats(final SortAndFormats sortAndFormats) {
        return sortAndFormats == null ? null : sortAndFormats.formats;
    }

    private void reduceCollectorResults(
        final QuerySearchResult result,
        final TopDocsAndMaxScore topDocsAndMaxScore,
        final DocValueFormat[] docValueFormats
    ) {
        // this is case of first collector, query result object doesn't have any top docs set, so we can
        // just set new top docs without merge
        // this call is effectively checking if QuerySearchResult.topDoc is null. using it in such way because
        // getter throws exception in case topDocs is null
        if (result.hasConsumedTopDocs()) {
            result.topDocs(topDocsAndMaxScore, docValueFormats);
            return;
        }
        // in this case top docs are already present in result, and we need to merge next result object with what we have.
        // if collector doesn't have any hits we can just skip it and save some cycles by not doing merge
        if (topDocsAndMaxScore.topDocs.totalHits.value == 0) {
            return;
        }
        // we need to do actual merge because query result and current collector both have some score hits
        TopDocsAndMaxScore originalTotalDocsAndHits = result.topDocs();
        TopDocsAndMaxScore mergeTopDocsAndMaxScores = topDocsMerger.merge(originalTotalDocsAndHits, topDocsAndMaxScore);
        result.topDocs(mergeTopDocsAndMaxScores, docValueFormats);
    }

    /**
     * For collection of search results, return a single one that has results from all individual result objects.
     * @param results collection of search results
     * @return single search result that represents all results as one object
     */
    private ReduceableSearchResult reduceSearchResults(final List<ReduceableSearchResult> results) {
        return (result) -> {
            for (ReduceableSearchResult r : results) {
                // call reduce for results of each single collector, this will update top docs in query result
                r.reduce(result);
            }
        };
    }

    /**
     * Get maximum subquery results count to be collected from each shard.
     * @param searchContext search context that contains pagination depth
     * @return results size to collected
     */
    private static int getSubqueryResultsRetrievalSize(final SearchContext searchContext) {
        HybridQuery hybridQuery = getHybridQueryFromAbstractQuery(searchContext.query());
        Integer paginationDepth = hybridQuery.getPaginationDepth();
        // Pagination is expected to work only pagination_depth is provided to hold the reference of search result.
        if (searchContext.from() > 0 && (Objects.isNull(paginationDepth))) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "pagination_depth is missing in the search request"));
        }
        if (paginationDepth != null) {
            return paginationDepth;
        } else {
            // Switch to from+size retrieval size when pagination_depth is null.
            return searchContext.from() + searchContext.size();
        }
    }

    private static HybridQuery getHybridQueryFromAbstractQuery(Query query) {
        HybridQuery hybridQuery;
        // In case of nested fields and alias filter, hybrid query is wrapped under bool query and lies in the first clause.
        if (query instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            hybridQuery = (HybridQuery) booleanQuery.clauses().get(0).getQuery();
        } else {
            hybridQuery = (HybridQuery) query;
        }
        return hybridQuery;
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
            Weight filteringWeight,
            SearchContext searchContext
        ) {
            super(
                numHits,
                hitsThresholdChecker,
                trackTotalHitsUpTo,
                searchContext.sort(),
                filteringWeight,
                new TopDocsMerger(searchContext.sort()),
                searchContext.searchAfter(),
                searchContext
            );
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
            Weight filteringWeight,
            SearchContext searchContext
        ) {
            super(
                numHits,
                hitsThresholdChecker,
                trackTotalHitsUpTo,
                searchContext.sort(),
                filteringWeight,
                new TopDocsMerger(searchContext.sort()),
                searchContext.searchAfter(),
                searchContext
            );
        }
    }
}
