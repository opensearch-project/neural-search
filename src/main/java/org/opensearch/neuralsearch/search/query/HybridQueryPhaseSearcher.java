/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
@Log4j2
public class HybridQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {

    private Function<List<TopDocs>, TotalHits> totalHitsSupplier;
    private Function<List<TopDocs>, Float> maxScoreSupplier;
    protected SortAndFormats sortAndFormats;

    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (query instanceof HybridQuery) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
        return super.searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    protected boolean searchWithCollector(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        log.debug(String.format(Locale.ROOT, "searching with custom doc collector, shard %s", searchContext.shardTarget().getShardId()));

        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        collectors.addFirst(topDocsFactory);

        final IndexReader reader = searchContext.searcher().getIndexReader();
        int totalNumDocs = Math.max(0, reader.numDocs());
        if (searchContext.size() == 0) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(query, collector);
            return false;
        }
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        final boolean rescore = !searchContext.rescore().isEmpty();
        if (rescore) {
            assert searchContext.sort() == null;
            for (RescoreContext rescoreContext : searchContext.rescore()) {
                numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
            }
        }

        final QuerySearchResult queryResult = searchContext.queryResult();

        final HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(
            numDocs,
            new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo()))
        );
        totalHitsSupplier = topDocs -> {
            int trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();
            final TotalHits.Relation relation = trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;
            if (topDocs == null || topDocs.size() == 0) {
                return new TotalHits(0, relation);
            }
            long maxTotalHits = topDocs.get(0).totalHits.value;
            for (TopDocs topDoc : topDocs) {
                maxTotalHits = Math.max(maxTotalHits, topDoc.totalHits.value);
            }
            return new TotalHits(maxTotalHits, relation);
        };
        maxScoreSupplier = topDocs -> {
            if (topDocs.size() == 0) {
                return Float.NaN;
            } else {
                return topDocs.stream()
                    .map(docs -> docs.scoreDocs.length == 0 ? new ScoreDoc(-1, 0.0f) : docs.scoreDocs[0])
                    .map(scoreDoc -> scoreDoc.score)
                    .max(Float::compare)
                    .get();
            }
        };
        sortAndFormats = searchContext.sort();

        searcher.search(query, collector);

        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }

        setTopDocsInQueryResult(queryResult, collector);

        return rescore;
    }

    void setTopDocsInQueryResult(final QuerySearchResult queryResult, final HybridTopScoreDocCollector collector) {
        final List<TopDocs> topDocs = collector.topDocs();
        float maxScore = maxScoreSupplier.apply(topDocs);
        final TopDocs newTopDocs = new CompoundTopDocs(totalHitsSupplier.apply(topDocs), topDocs);
        final TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(newTopDocs, maxScore);
        queryResult.topDocs(topDocsAndMaxScore, getSortValueFormats());
    }

    private DocValueFormat[] getSortValueFormats() {
        return sortAndFormats == null ? null : sortAndFormats.formats;
    }
}
