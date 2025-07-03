/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.query.exception.HybridSearchRescoreQueryException;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createCollapseValueDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createCollapseValueStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createSortFieldsForDelimiterResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;

@Log4j2
public class HybridSearchCollectorResultUtil {
    private final HybridCollectorResultsUtilParams hybridSearchCollectorResultsDTO;
    private final HybridSearchCollector hybridSearchCollector;

    public HybridSearchCollectorResultUtil(
        final HybridCollectorResultsUtilParams hybridSearchCollectorResultsDTO,
        final HybridSearchCollector hybridSearchCollector
    ) {
        this.hybridSearchCollectorResultsDTO = hybridSearchCollectorResultsDTO;
        this.hybridSearchCollector = hybridSearchCollector;
    }

    public TopDocsAndMaxScore getTopDocsAndAndMaxScore() throws IOException {
        List topDocs = hybridSearchCollector.topDocs();
        if (hybridSearchCollectorResultsDTO.isSortEnabled() || hybridSearchCollectorResultsDTO.isCollapseEnabled()) {
            return getSortedTopDocsAndMaxScore(topDocs);
        }
        return getTopDocsAndMaxScore(topDocs);
    }

    private TopDocsAndMaxScore getSortedTopDocsAndMaxScore(List<TopFieldDocs> topDocs) {
        final SortField sortFields[] = hybridSearchCollectorResultsDTO.getSortFields();
        TopDocs sortedTopDocs = getNewTopFieldDocs(getTotalHits(topDocs), topDocs, sortFields);
        return new TopDocsAndMaxScore(sortedTopDocs, hybridSearchCollector.getMaxScore());
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
            return new TopFieldDocs(
                totalHits,
                new FieldDoc[0],
                sortFields == null ? new SortField[] { new SortField(null, SortField.Type.SCORE) } : sortFields
            );
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

        if (hybridSearchCollectorResultsDTO.isCollapseEnabled()) {
            ArrayList<Object> collapseValues = new ArrayList<>();
            String collapseField = "";
            ArrayList<FieldDoc> fieldDocs = new ArrayList<>();

            List<FieldDoc> result = new ArrayList<>();
            Object[] fields = createSortFieldsForDelimiterResults(topFieldDocs.getFirst().fields);
            result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, fields));
            collapseValues.add(new BytesRef(createCollapseValueStartStopElementForHybridSearchResults()));
            for (TopDocs topDoc : topFieldDocs) {
                CollapseTopFieldDocs collapseTopFieldDoc = (CollapseTopFieldDocs) topDoc;
                collapseField = collapseTopFieldDoc.field;
                if (Objects.isNull(topDoc) || Objects.isNull(topDoc.scoreDocs)) {
                    result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, fields));
                    continue;
                }

                List<FieldDoc> fieldDocsPerQuery = new ArrayList<>();
                for (ScoreDoc scoreDoc : collapseTopFieldDoc.scoreDocs) {
                    fieldDocsPerQuery.add((FieldDoc) scoreDoc);
                }
                result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, fields));
                result.addAll(fieldDocsPerQuery);
                collapseValues.add(new BytesRef(createCollapseValueDelimiterElementForHybridSearchResults()));
                collapseValues.addAll(Arrays.asList(collapseTopFieldDoc.collapseValues));
            }
            result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, fields));
            collapseValues.add(new BytesRef(createCollapseValueStartStopElementForHybridSearchResults()));
            fieldDocs.addAll(result);

            return new CollapseTopFieldDocs(
                collapseField,
                totalHits,
                fieldDocs.toArray(new FieldDoc[0]),
                topFieldDocs.getFirst().fields,
                collapseValues.toArray(new Object[0])
            );

        } else {
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
    }

    private TotalHits getTotalHits(final List<?> topDocs) {
        final TotalHits.Relation relation = hybridSearchCollectorResultsDTO
            .getTrackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO;

        if (topDocs == null || topDocs.isEmpty()) {
            return new TotalHits(0, relation);
        }

        return new TotalHits(hybridSearchCollector.getTotalHits(), relation);
    }

    private TopDocsAndMaxScore getTopDocsAndMaxScore(List<TopDocs> topDocs) {
        if (shouldRescore()) {
            topDocs = rescore(topDocs);
        }
        float maxScore = calculateMaxScore(topDocs, hybridSearchCollector.getMaxScore());
        TopDocs finalTopDocs = getNewTopDocs(getTotalHits(topDocs), topDocs);
        return new TopDocsAndMaxScore(finalTopDocs, maxScore);
    }

    private TopDocs getNewTopDocs(final TotalHits totalHits, final List<TopDocs> topDocs) {
        boolean isCollapseEnabled = topDocs.isEmpty() == false && topDocs.get(0) instanceof CollapseTopFieldDocs;
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        ArrayList<Object> collapseValues = new ArrayList<>();
        String collapseField = "";
        ArrayList<FieldDoc> fieldDocs = new ArrayList<>();
        ArrayList<SortField> sortFields = new ArrayList<>();
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

            if (isCollapseEnabled) {
                List<FieldDoc> result = new ArrayList<>();
                Object[] fields = new Object[0];
                result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, fields));
                collapseValues.add(0);
                for (TopDocs topDoc : topDocs) {
                    CollapseTopFieldDocs collapseTopFieldDoc = (CollapseTopFieldDocs) topDoc;
                    collapseField = collapseTopFieldDoc.field;
                    sortFields.addAll(Arrays.asList(collapseTopFieldDoc.fields));
                    if (Objects.isNull(topDoc) || Objects.isNull(topDoc.scoreDocs)) {
                        result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, fields));
                        continue;
                    }

                    List<FieldDoc> fieldDocsPerQuery = new ArrayList<>();
                    for (ScoreDoc scoreDoc : collapseTopFieldDoc.scoreDocs) {
                        fieldDocsPerQuery.add(new FieldDoc(scoreDoc.doc, scoreDoc.score, new Object[0]));
                    }
                    result.add(createFieldDocDelimiterElementForHybridSearchResults(delimiterDocId, fields));
                    result.addAll(fieldDocsPerQuery);
                    // Dummy delimiter element
                    collapseValues.add(0);
                    collapseValues.addAll(Arrays.asList(collapseTopFieldDoc.collapseValues));

                }
                result.add(createFieldDocStartStopElementForHybridSearchResults(delimiterDocId, fields));
                collapseValues.add(0);
                fieldDocs.addAll(result);
            } else {
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
        }
        if (isCollapseEnabled) {
            return new CollapseTopFieldDocs(
                collapseField,
                totalHits,
                fieldDocs.toArray(new FieldDoc[0]),
                sortFields.toArray(new SortField[0]),
                collapseValues.toArray(new Object[0])
            );
        }
        return new TopDocs(totalHits, scoreDocs);
    }

    /**
     * Calculates the maximum score from the provided TopDocs, considering rescoring.
     */
    private float calculateMaxScore(List<TopDocs> topDocsList, float initialMaxScore) {
        List<RescoreContext> rescoreContexts = hybridSearchCollectorResultsDTO.getRescoreContexts();
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

    private boolean shouldRescore() {
        List<RescoreContext> rescoreContexts = hybridSearchCollectorResultsDTO.getRescoreContexts();
        return CollectionUtils.isEmpty(rescoreContexts) == false;
    }

    private List<TopDocs> rescore(List<TopDocs> topDocs) {
        List<TopDocs> rescoredTopDocs = topDocs;
        for (RescoreContext ctx : hybridSearchCollectorResultsDTO.getRescoreContexts()) {
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
                result.add(ctx.rescorer().rescore(topDoc, hybridSearchCollectorResultsDTO.getSearchContext().searcher(), ctx));
            } catch (IOException exception) {
                log.error("rescore failed for hybrid query in collector_manager.reduce call", exception);
                throw new HybridSearchRescoreQueryException(exception);
            }
        }
        return result;
    }

    public void reduceCollectorResults(final QuerySearchResult result, final TopDocsAndMaxScore topDocsAndMaxScore) {
        // this is case of first collector, query result object doesn't have any top docs set, so we can
        // just set new top docs without merge
        // this call is effectively checking if QuerySearchResult.topDoc is null. using it in such way because
        // getter throws exception in case topDocs is null
        if (result.hasConsumedTopDocs()) {
            result.topDocs(topDocsAndMaxScore, hybridSearchCollectorResultsDTO.getDocValueFormats());
            return;
        }
        // in this case top docs are already present in result, and we need to merge next result object with what we have.
        // if collector doesn't have any hits we can just skip it and save some cycles by not doing merge
        if (topDocsAndMaxScore.topDocs.totalHits.value() == 0) {
            return;
        }
        // we need to do actual merge because query result and current collector both have some score hits
        TopDocsAndMaxScore originalTotalDocsAndHits = result.topDocs();
        TopDocsAndMaxScore mergeTopDocsAndMaxScores = hybridSearchCollectorResultsDTO.getTopDocsMerger()
            .merge(originalTotalDocsAndHits, topDocsAndMaxScore);
        result.topDocs(mergeTopDocsAndMaxScores, hybridSearchCollectorResultsDTO.getDocValueFormats());
    }
}
