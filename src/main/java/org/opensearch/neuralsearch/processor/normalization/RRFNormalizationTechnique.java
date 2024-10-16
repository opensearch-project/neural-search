/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;

/**
 * Abstracts calculation of rank scores for each document returned as part of
 * reciprocal rank fusion. Rank scores are summed across subqueries in combination classes
 */
@ToString(onlyExplicitlyIncluded = true)
public class RRFNormalizationTechnique implements ScoreNormalizationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    public static final int DEFAULT_RANK_CONSTANT = 60;
    public static final String PARAM_NAME_RANK_CONSTANT = "rank_constant";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_RANK_CONSTANT);

    private final int rankConstant;

    public RRFNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        scoreNormalizationUtil.validateParams(params, SUPPORTED_PARAMS);
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
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (TopDocs topDocs : topDocsPerSubQuery) {
                int docsCountPerSubQuery = topDocs.scoreDocs.length;
                ScoreDoc[] scoreDocs = topDocs.scoreDocs;
                for (int j = 0; j < docsCountPerSubQuery; j++) {
                    scoreDocs[j].score = (1.f / (float) (rankConstant + j + 1));
                }
            }
        }
    }

    private int getRankConstant(final Map<String, Object> params) {
        if (!params.containsKey(PARAM_NAME_RANK_CONSTANT)) {
            return DEFAULT_RANK_CONSTANT;
        }
        int rankConstant = getParamAsInteger(params, PARAM_NAME_RANK_CONSTANT);
        validateRankConstant(rankConstant);
        return rankConstant;
    }

    private void validateRankConstant(final int rankConstant) {
        boolean isOutOfRange = rankConstant < 1 || rankConstant >= 10_000;
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "rank constant must be in the interval between 1 and 10000, submitted rank constant: %d",
                    rankConstant
                )
            );
        }
    }

    public static int getParamAsInteger(final Map<String, Object> parameters, final String fieldName) {
        try {
            return NumberUtils.createInteger(String.valueOf(parameters.get(fieldName)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "parameter [%s] must be an integer", fieldName));
        }
    }
}
