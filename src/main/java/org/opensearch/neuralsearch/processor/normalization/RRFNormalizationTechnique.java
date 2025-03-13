/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.TriConsumer;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.GeometricMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.HarmonicMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;

/**
 * Abstracts calculation of rank scores for each document returned as part of
 * reciprocal rank fusion. Rank scores are summed across subqueries in combination classes.
 */
@ToString(onlyExplicitlyIncluded = true)
public class RRFNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final String PARAM_NAME_RANK_CONSTANT = "rank_constant";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_RANK_CONSTANT);
    private static final int MIN_RANK_CONSTANT = 1;
    private static final int MAX_RANK_CONSTANT = 10_000;
    private static final Range<Integer> RANK_CONSTANT_RANGE = Range.of(MIN_RANK_CONSTANT, MAX_RANK_CONSTANT);
    @ToString.Include
    private final int rankConstant;

    public RRFNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        scoreNormalizationUtil.validateParameters(params, SUPPORTED_PARAMS, Map.of());
        rankConstant = getRankConstant(params);
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
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            processTopDocs(compoundQueryTopDocs, (docId, score, subQueryIndex) -> {});
        }
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s, rank_constant [%s]", TECHNIQUE_NAME, rankConstant);
    }

    @Override
    public void validateCombinationTechnique(ScoreCombinationTechnique combinationTechnique) {
        switch (combinationTechnique.techniqueName()) {
            case ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME, GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
                HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME:
                // These are the supported technique, so we do nothing
                break;
            default:
                throw new IllegalArgumentException(
                    "Z Score does not support the provided combination technique {}: Supported techniques are arithmetic_mean, geometric_mean and harmonic_mean"
                        + combinationTechnique.techniqueName()
                );
        }
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(List<CompoundTopDocs> queryTopDocs) {
        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();

        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
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
                )
            );
        }

        return getDocIdAtQueryForNormalization(normalizedScores, this);
    }

    private void processTopDocs(CompoundTopDocs compoundQueryTopDocs, TriConsumer<DocIdAtSearchShard, Float, Integer> scoreProcessor) {
        if (Objects.isNull(compoundQueryTopDocs)) {
            return;
        }

        List<TopDocs> topDocsList = compoundQueryTopDocs.getTopDocs();
        SearchShard searchShard = compoundQueryTopDocs.getSearchShard();

        for (int topDocsIndex = 0; topDocsIndex < topDocsList.size(); topDocsIndex++) {
            processTopDocsEntry(topDocsList.get(topDocsIndex), searchShard, topDocsIndex, scoreProcessor);
        }
    }

    private void processTopDocsEntry(
        TopDocs topDocs,
        SearchShard searchShard,
        int topDocsIndex,
        TriConsumer<DocIdAtSearchShard, Float, Integer> scoreProcessor
    ) {
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            float normalizedScore = calculateNormalizedScore(Arrays.asList(topDocs.scoreDocs).indexOf(scoreDoc));
            DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(scoreDoc.doc, searchShard);
            scoreProcessor.apply(docIdAtSearchShard, normalizedScore, topDocsIndex);
            scoreDoc.score = normalizedScore;
        }
    }

    private float calculateNormalizedScore(int position) {
        return BigDecimal.ONE.divide(BigDecimal.valueOf(rankConstant + position + 1), 10, RoundingMode.HALF_UP).floatValue();
    }

    private int getRankConstant(final Map<String, Object> params) {
        if (Objects.isNull(params) || !params.containsKey(PARAM_NAME_RANK_CONSTANT)) {
            return DEFAULT_RANK_CONSTANT;
        }
        int rankConstant = getParamAsInteger(params, PARAM_NAME_RANK_CONSTANT);
        validateRankConstant(rankConstant);
        return rankConstant;
    }

    private void validateRankConstant(final int rankConstant) {
        if (!RANK_CONSTANT_RANGE.contains(rankConstant)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "rank constant must be in the interval between 1 and 10000, submitted rank constant: %d",
                    rankConstant
                )
            );
        }
    }

    private static int getParamAsInteger(final Map<String, Object> parameters, final String fieldName) {
        try {
            return NumberUtils.createInteger(String.valueOf(parameters.get(fieldName)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "parameter [%s] must be an integer", fieldName));
        }
    }
}
