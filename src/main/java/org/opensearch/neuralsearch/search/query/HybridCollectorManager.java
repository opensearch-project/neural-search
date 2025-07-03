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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.FieldDoc;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.FilteredCollector;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactory;
import org.opensearch.neuralsearch.search.collector.HybridCollectorFactoryDTO;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopFieldDocSortCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopScoreDocCollector;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.MultiCollectorWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.neuralsearch.util.HybridQueryUtil.isHybridQueryWrappedInBooleanQuery;

/**
 * Collector manager based on HybridTopScoreDocCollector that allows users to parallelize counting the number of hits.
 * In most cases it will be wrapped in MultiCollectorManager.
 */
@RequiredArgsConstructor
@Log4j2
public class HybridCollectorManager implements CollectorManager<Collector, ReduceableSearchResult> {

    private final int numHits;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final SortAndFormats sortAndFormats;
    @Nullable
    private final Weight filterWeight;
    private static final float boostFactor = 1f;
    @Nullable
    private final FieldDoc after;
    private final SearchContext searchContext;

    private final Set<Class<?>> VALID_COLLECTOR_TYPES = Set.of(
        HybridTopScoreDocCollector.class,
        HybridTopFieldDocSortCollector.class,
        HybridCollapsingTopDocsCollector.class
    );

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

        return new HybridCollectorManager(
            numDocs,
            new HitsThresholdChecker(Math.max(numDocs, trackTotalHitsUpTo)),
            searchContext.sort(),
            filteringWeight,
            searchContext.searchAfter(),
            searchContext
        );
    }

    @Override
    public Collector newCollector() {
        Collector hybridCollector = HybridCollectorFactory.createCollector(
            HybridCollectorFactoryDTO.builder()
                .sortAndFormats(sortAndFormats)
                .searchContext(searchContext)
                .hitsThresholdChecker(hitsThresholdChecker)
                .numHits(numHits)
                .after(after)
                .build()
        );
        // Check if filterWeight is present. If it is present then return wrap Hybrid Sort collector object underneath the FilteredCollector
        // object and return it.
        return Objects.nonNull(filterWeight) ? new FilteredCollector(hybridCollector, filterWeight) : hybridCollector;
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
    public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
        final List<HybridSearchCollector> hybridSearchCollectors = getHybridSearchCollectors(collectors);
        if (hybridSearchCollectors.isEmpty()) {
            throw new IllegalStateException("cannot collect results of hybrid search query, there are no proper collectors");
        }
        return reduceSearchResults(getSearchResults(hybridSearchCollectors));
    }

    private List<ReduceableSearchResult> getSearchResults(final List<HybridSearchCollector> hybridSearchCollectors) throws IOException {
        List<ReduceableSearchResult> results = new ArrayList<>();
        HybridCollectorResultsUtilParams hybridCollectorResultsUtilParams = new HybridCollectorResultsUtilParams.Builder().searchContext(
            searchContext
        ).build();
        for (HybridSearchCollector collector : hybridSearchCollectors) {
            HybridSearchCollectorResultUtil hybridSearchCollectorResultUtil = new HybridSearchCollectorResultUtil(
                hybridCollectorResultsUtilParams,
                collector
            );
            TopDocsAndMaxScore topDocsAndMaxScore = hybridSearchCollectorResultUtil.getTopDocsAndAndMaxScore();
            results.add((QuerySearchResult result) -> hybridSearchCollectorResultUtil.reduceCollectorResults(result, topDocsAndMaxScore));
        }
        return results;
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
            } else if (isHybridNonFilteredCollector(collector)) {
                hybridSearchCollectors.add((HybridSearchCollector) collector);
            } else if (isHybridFilteredCollector(collector)) {
                hybridSearchCollectors.add((HybridSearchCollector) ((FilteredCollector) collector).getCollector());
            }
        }
        return hybridSearchCollectors;
    }

    private boolean isHybridNonFilteredCollector(Collector collector) {
        return VALID_COLLECTOR_TYPES.stream().anyMatch(type -> type.isInstance(collector));
    }

    private boolean isHybridFilteredCollector(Collector collector) {
        return collector instanceof FilteredCollector
            && VALID_COLLECTOR_TYPES.stream().anyMatch(type -> type.isInstance(((FilteredCollector) collector).getCollector()));
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
        HybridQuery hybridQuery = unwrapHybridQuery(searchContext);
        Integer paginationDepth = hybridQuery.getQueryContext().getPaginationDepth();

        // Pagination is expected to work only when pagination_depth is provided in the search request.
        if (Objects.isNull(paginationDepth) && searchContext.from() > 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "pagination_depth param is missing in the search request"));
        }

        if (Objects.nonNull(paginationDepth)) {
            return paginationDepth;
        }

        // Switch to from+size retrieval size during standard hybrid query execution where from is 0.
        return searchContext.size();
    }

    /**
     * Unwraps a HybridQuery from either a direct query or a nested BooleanQuery
     */
    private static HybridQuery unwrapHybridQuery(final SearchContext searchContext) {
        HybridQuery hybridQuery;
        Query query = searchContext.query();
        // In case of nested fields and alias filter, hybrid query is wrapped under bool query and lies in the first clause.
        if (isHybridQueryWrappedInBooleanQuery(searchContext, searchContext.query())) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            hybridQuery = (HybridQuery) booleanQuery.clauses().get(0).query();
        } else {
            hybridQuery = (HybridQuery) query;
        }
        return hybridQuery;
    }
}
