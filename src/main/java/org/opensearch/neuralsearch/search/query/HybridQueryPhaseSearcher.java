/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.query;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.TopDocsCollectorContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

import com.google.common.annotations.VisibleForTesting;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
@Log4j2
public class HybridQueryPhaseSearcher extends QueryPhaseSearcherWrapper {

    final static int MAX_NESTED_SUBQUERY_LIMIT = 50;

    public HybridQueryPhaseSearcher() {
        super();
    }

    public boolean searchWith(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        if (isHybridQuery(query, searchContext)) {
            query = extractHybridQuery(searchContext, query);
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
        validateQuery(query);
        return super.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    private boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery) {
            return true;
        } else if (hasNestedFieldOrNestedDocs(query, searchContext) && mightBeWrappedHybridQuery(query)) {
            // checking if this is a hybrid query that is wrapped into a Bool query by core Opensearch code
            // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/DefaultSearchContext.java#L367-L370.
            // main reason for that is performance optimization, at time of writing we are ok with loosing on performance if that's unblocks
            // hybrid query for indexes with nested field types.
            // in such case we consider query a valid hybrid query. Later in the code we will extract it and execute as a main query for
            // this search request.
            // below is sample structure of such query:
            //
            // Boolean {
            // should: {
            // hybrid: {
            // sub_query1 {}
            // sub_query2 {}
            // }
            // }
            // filter: {
            // exists: {
            // field: "_primary_term"
            // }
            // }
            // }
            if (query instanceof BooleanQuery == false) {
                return false;
            }
            return ((BooleanQuery) query).clauses()
                .stream()
                .filter(clause -> clause.getQuery() instanceof HybridQuery == false)
                .allMatch(
                    clause -> clause.getOccur() == BooleanClause.Occur.FILTER
                        && clause.getQuery() instanceof FieldExistsQuery
                        && SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(((FieldExistsQuery) clause.getQuery()).getField())
                );
        }
        return false;
    }

    private boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private boolean mightBeWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.getQuery() instanceof HybridQuery);
    }

    private Query extractHybridQuery(final SearchContext searchContext, final Query query) {
        if (hasNestedFieldOrNestedDocs(query, searchContext)
            && mightBeWrappedHybridQuery(query)
            && ((BooleanQuery) query).clauses().size() > 0) {
            // extract hybrid query and replace bool with hybrid query
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            Query hybridQuery = booleanClauses.stream().findFirst().get().getQuery();
            if (!(hybridQuery instanceof HybridQuery)) {
                throw new IllegalStateException("cannot find hybrid type query in expected location");
            }
            return hybridQuery;
        }
        return query;
    }

    private void validateQuery(final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            for (BooleanClause booleanClause : booleanClauses) {
                validateNestedBooleanQuery(booleanClause.getQuery(), 1);
            }
        }
    }

    private void validateNestedBooleanQuery(final Query query, int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level >= MAX_NESTED_SUBQUERY_LIMIT) {
            throw new IllegalStateException("reached max nested query limit, cannot process query");
        }
        if (query instanceof BooleanQuery) {
            for (BooleanClause booleanClause : ((BooleanQuery) query).clauses()) {
                validateNestedBooleanQuery(booleanClause.getQuery(), level + 1);
            }
        }
    }

    @VisibleForTesting
    protected boolean searchWithCollector(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        log.debug("searching with custom doc collector, shard {}", searchContext.shardTarget().getShardId());

        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        collectors.addFirst(topDocsFactory);
        if (searchContext.size() == 0) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(query, collector);
            return false;
        }
        final IndexReader reader = searchContext.searcher().getIndexReader();
        int totalNumDocs = Math.max(0, reader.numDocs());
        int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
        final boolean shouldRescore = !searchContext.rescore().isEmpty();
        if (shouldRescore) {
            for (RescoreContext rescoreContext : searchContext.rescore()) {
                numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
            }
        }

        final QuerySearchResult queryResult = searchContext.queryResult();

        final HybridTopScoreDocCollector collector = new HybridTopScoreDocCollector(
            numDocs,
            new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo()))
        );

        searcher.search(query, collector);

        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }

        setTopDocsInQueryResult(queryResult, collector, searchContext);

        return shouldRescore;
    }

    private void setTopDocsInQueryResult(
        final QuerySearchResult queryResult,
        final HybridTopScoreDocCollector collector,
        final SearchContext searchContext
    ) {
        final List<TopDocs> topDocs = collector.topDocs();
        final float maxScore = getMaxScore(topDocs);
        final boolean isSingleShard = searchContext.numberOfShards() == 1;
        final TopDocs newTopDocs = getNewTopDocs(getTotalHits(searchContext, topDocs, isSingleShard), topDocs);
        final TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(newTopDocs, maxScore);
        queryResult.topDocs(topDocsAndMaxScore, getSortValueFormats(searchContext.sort()));
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

    private TotalHits getTotalHits(final SearchContext searchContext, final List<TopDocs> topDocs, final boolean isSingleShard) {
        int trackTotalHitsUpTo = searchContext.trackTotalHitsUpTo();
        final TotalHits.Relation relation = trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
        if (topDocs == null || topDocs.isEmpty()) {
            return new TotalHits(0, relation);
        }
        long maxTotalHits = topDocs.get(0).totalHits.value;
        int totalSize = 0;
        for (TopDocs topDoc : topDocs) {
            maxTotalHits = Math.max(maxTotalHits, topDoc.totalHits.value);
            if (isSingleShard) {
                totalSize += topDoc.totalHits.value + 1;
            }
        }
        // add 1 qty per each sub-query and + 2 for start and stop delimiters
        totalSize += 2;
        if (isSingleShard) {
            // for single shard we need to update total size as this is how many docs are fetched in Fetch phase
            searchContext.size(totalSize);
        }

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
}
