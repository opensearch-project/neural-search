/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Map;
import java.util.Locale;

import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;

/**
 * Abstracts calculation of rank scores for each document returned as part of
 * reciprocal rank fusion. Rank scores are summed across subqueries in combination
 * classes
 */
@ToString(onlyExplicitlyIncluded = true)
public class RRFNormalizationTechnique implements ScoreNormalizationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    public static final String RANK_CONSTANT = "rank_constant";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(RANK_CONSTANT);
    private int rankConstant;
    private static final RRFParamUtil rrfParamUtil = new RRFParamUtil();
    private static final int DEFAULT_RANK_CONSTANT = 60;

    /*public RRFParamsParse(final Map<String, Object> params, final RRFParamUtil rrfUtil) {
        rrfUtil.validateRRFParams(params, SUPPORTED_PARAMS);
        rankConstant = rrfUtil.getRankConstant(params);
    }*/

    public int getRankConstant(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return DEFAULT_RANK_CONSTANT;
        }
        // get rankConstant, we don't need to check for instance as it's done during validation
        // input capable of handling list but will only return first item as int or the default
        // int value of 60 if empty
        int rankConstant = (int) params.getOrDefault(RANK_CONSTANT, DEFAULT_RANK_CONSTANT);
        validateRankConstant(rankConstant);
        return rankConstant;
    }

    /*public void validateRRFParams(final Map<String, Object> actualParams, final Set<String> supportedParams) {
        if (Objects.isNull(actualParams) || actualParams.isEmpty()) {
            return;
        }
    }*/
    private void validateRankConstant(final int rankConstant) {
        boolean isOutOfRange = rankConstant < 1 || rankConstant >= Integer.MAX_VALUE;
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "rank constant must be >= 1 and < (2^31)-1, submitted rank constant: %d", rankConstant)
            );
        }
    }

    @Override
    public void normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        rankConstant = normalizeScoresDTO.getRankConstant();
        validateRankConstant(rankConstant);
        // rankConstant = rrfParamUtil.getRankConstant(rrfParams);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numSubQueriesBound = topDocsPerSubQuery.size();
            for (int index = 0; index < numSubQueriesBound; index++) {
                int numDocsPerSubQueryBound = topDocsPerSubQuery.get(index).scoreDocs.length;
                for (int j = 0; j < numDocsPerSubQueryBound; j++) {
                    topDocsPerSubQuery.get(index).scoreDocs[j].score = (float) (1 / (rankConstant + j + 1));
                }
            }
        }
    }

}
