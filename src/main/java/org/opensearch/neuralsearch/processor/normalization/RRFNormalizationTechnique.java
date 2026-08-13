/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Set;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.TriConsumer;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.dto.ExplainDTO;
import org.opensearch.neuralsearch.processor.dto.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;

/**
 * Abstracts calculation of rank scores for each document returned as part of
 * reciprocal rank fusion. Rank scores are summed across subqueries in combination classes.
 * <p>
 * This class owns the traversal of shard side {@link CompoundTopDocs}; the RRF arithmetic itself lives in
 * {@link RRFScoreNormalizer} so that coordinator side fusion can apply identical logic to its own result shape.
 */
@ToString(onlyExplicitlyIncluded = true)
@Log4j2
public class RRFNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    public static final int DEFAULT_RANK_CONSTANT = RRFScoreNormalizer.DEFAULT_RANK_CONSTANT;
    public static final String PARAM_NAME_RANK_CONSTANT = RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT;
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_RANK_CONSTANT);
    @ToString.Include
    private final int rankConstant;
    // Comparator to compare ShardResultPerSubQuery
    private static final Comparator<ShardResultPerSubQuery> comparator = Comparator.comparing(
        (ShardResultPerSubQuery sr) -> sr.scoreDoc,
        ScoreDoc.COMPARATOR
    ).thenComparingInt(sr -> sr.referenceShardId);

    public RRFNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        scoreNormalizationUtil.validateParameters(params, SUPPORTED_PARAMS, Map.of());
        rankConstant = RRFScoreNormalizer.resolveRankConstant(params);
    }

    /**
     * Reciprocal Rank Fusion normalization technique
     * @param normalizeScoresDTO is a data transfer object that contains queryTopDocs
     * original query results from multiple shards and multiple sub-queries, ScoreNormalizationTechnique,
     * and nullable rankConstant, which has a default value of 60 if not specified by user
     * algorithm as follows, where document_n_score is the new score for each document in queryTopDocs
     * and subquery_result_rank is the position in the array of documents returned for each subquery
     * (j + 1 is used to adjust for 0 indexing)
     * document_n_score = 1 / (rankConstant + subquery_result_rank)
     * document scores are summed in combination step
     */
    @Override
    public void normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();

        Map<Integer, Map<String, Integer>> sortedDocIdsPerSubqueryByGlobalRank = normalizeScoresDTO.isSingleShard()
            ? Map.of()
            : sortDocumentsAsPerGlobalRankInIndividualQuery(queryTopDocs);

        for (int referenceShardId = 0; referenceShardId < queryTopDocs.size(); referenceShardId++) {
            processTopDocs(
                queryTopDocs.get(referenceShardId),
                (docId, score, subQueryIndex) -> {},
                sortedDocIdsPerSubqueryByGlobalRank,
                referenceShardId
            );
        }
    }

    private Map<Integer, Map<String, Integer>> sortDocumentsAsPerGlobalRankInIndividualQuery(
        @NonNull final List<CompoundTopDocs> queryTopDocs
    ) {
        // Map: subQueryIndex -> PQ that contains ScoreDocs from all shards
        Map<Integer, PriorityQueue<ShardResultPerSubQuery>> scoreDocsPerSubquery = new HashMap<>();
        for (int referenceShardId = 0; referenceShardId < queryTopDocs.size(); referenceShardId++) {
            CompoundTopDocs compoundTopDocs = queryTopDocs.get(referenceShardId);
            if (Objects.isNull(compoundTopDocs)) {
                continue;
            }
            // List of subquery results
            List<TopDocs> topDocs = compoundTopDocs.getTopDocs();
            for (int topDocIndex = 0; topDocIndex < topDocs.size(); topDocIndex++) {
                TopDocs topDoc = topDocs.get(topDocIndex);
                scoreDocsPerSubquery.putIfAbsent(topDocIndex, new PriorityQueue<>(comparator));
                ScoreDoc[] scoreDocs = topDoc.scoreDocs;
                // Insert all the scoreDocs in priority queue that holds results across all shards for the subquery
                for (int scoreDocIndex = 0; scoreDocIndex < scoreDocs.length; scoreDocIndex++) {
                    scoreDocsPerSubquery.get(topDocIndex).add(new ShardResultPerSubQuery(scoreDocs[scoreDocIndex], referenceShardId));
                }
            }
        }

        Map<Integer, Map<String, Integer>> globallySortedDocIdMap = new HashMap<>();
        // Traverse all scoreDocs per subquery and add it in the map with docId-shardId order.
        // We created docId-shardId format because docId can be same across different shards but the combination is unique.
        for (Map.Entry<Integer, PriorityQueue<ShardResultPerSubQuery>> entry : scoreDocsPerSubquery.entrySet()) {
            int subQueryNumber = entry.getKey();
            PriorityQueue<ShardResultPerSubQuery> sortedScoreDocsAcrossAllShards = entry.getValue();
            globallySortedDocIdMap.putIfAbsent(
                subQueryNumber,
                RRFScoreNormalizer.drainToRanks(
                    sortedScoreDocsAcrossAllShards,
                    shardResultPerSubQuery -> shardResultPerSubQuery.scoreDoc.doc + "_" + shardResultPerSubQuery.referenceShardId
                )
            );
        }
        return globallySortedDocIdMap;
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s, rank_constant [%s]", TECHNIQUE_NAME, rankConstant);
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(final ExplainDTO explainDTO) {
        final List<CompoundTopDocs> queryTopDocs = explainDTO.getQueryTopDocs();
        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
        Map<Integer, Map<String, Integer>> sortedDocIdsPerSubqueryByGlobalRank = explainDTO.isSingleShard()
            ? Map.of()
            : sortDocumentsAsPerGlobalRankInIndividualQuery(queryTopDocs);
        for (int referenceShardId = 0; referenceShardId < queryTopDocs.size(); referenceShardId++) {
            CompoundTopDocs compoundQueryTopDocs = queryTopDocs.get(referenceShardId);
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numberOfSubQueries = topDocsPerSubQuery.size();
            processTopDocs(
                compoundQueryTopDocs,
                (docId, score, subQueryIndex) -> ScoreNormalizationUtil.setNormalizedScore(
                    normalizedScores,
                    docId,
                    subQueryIndex,
                    numberOfSubQueries,
                    score
                ),
                sortedDocIdsPerSubqueryByGlobalRank,
                referenceShardId
            );
        }

        return getDocIdAtQueryForNormalization(normalizedScores, this);
    }

    private void processTopDocs(
        CompoundTopDocs compoundQueryTopDocs,
        TriConsumer<DocIdAtSearchShard, Float, Integer> scoreProcessor,
        Map<Integer, Map<String, Integer>> sortedDocIdMapPerSubqueryByGlobalRank,
        int referenceShardId
    ) {
        if (Objects.isNull(compoundQueryTopDocs)) {
            return;
        }

        List<TopDocs> topDocsList = compoundQueryTopDocs.getTopDocs();
        SearchShard searchShard = compoundQueryTopDocs.getSearchShard();

        for (int topDocsIndex = 0; topDocsIndex < topDocsList.size(); topDocsIndex++) {
            Map<String, Integer> docIdToRankMap = sortedDocIdMapPerSubqueryByGlobalRank.isEmpty()
                ? Map.of()
                : sortedDocIdMapPerSubqueryByGlobalRank.get(topDocsIndex);
            processTopDocsEntry(topDocsList.get(topDocsIndex), searchShard, topDocsIndex, scoreProcessor, docIdToRankMap, referenceShardId);
        }
    }

    private void processTopDocsEntry(
        @NonNull TopDocs topDocs,
        SearchShard searchShard,
        int topDocsIndex,
        TriConsumer<DocIdAtSearchShard, Float, Integer> scoreProcessor,
        Map<String, Integer> docIdToRankMap,
        int referenceShardId
    ) {
        for (int position = 0; position < topDocs.scoreDocs.length; position++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[position];
            Integer rank = docIdToRankMap.isEmpty() ? position : docIdToRankMap.get(scoreDoc.doc + "_" + referenceShardId);
            if (Objects.isNull(rank) || rank < 0) {
                throw new IllegalStateException(
                    "Document not found in global ranking map: doc=" + scoreDoc.doc + ", shard=" + referenceShardId
                );
            }
            float normalizedScore = RRFScoreNormalizer.scoreForRank(rank, rankConstant);
            DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(scoreDoc.doc, searchShard);
            scoreProcessor.apply(docIdAtSearchShard, normalizedScore, topDocsIndex);
            scoreDoc.score = normalizedScore;
        }
    }

    /**
     * Record to store shard results per subquery
     * @param scoreDoc scoreDoc result
     * @param referenceShardId reference shard Id for identifying scoreDoc
     */
    private record ShardResultPerSubQuery(ScoreDoc scoreDoc, int referenceShardId) {
    }
}
