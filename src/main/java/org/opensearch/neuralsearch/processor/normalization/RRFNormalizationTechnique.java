/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.List;
import java.util.Objects;
import java.util.Locale;

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

    private void validateRankConstant(final int rankConstant) {
        boolean isOutOfRange = rankConstant < 1 || rankConstant >= Integer.MAX_VALUE;
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "rank constant must be >= 1 and < (2^31)-1, submitted rank constant: %d", rankConstant)
            );
        }
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
        final int rankConstant = normalizeScoresDTO.getRankConstant();
        validateRankConstant(rankConstant);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numSubQueriesBound = topDocsPerSubQuery.size();
            for (int index = 0; index < numSubQueriesBound; index++) {
                int numDocsPerSubQueryBound = topDocsPerSubQuery.get(index).scoreDocs.length;
                for (int j = 0; j < numDocsPerSubQueryBound; j++) {
                    topDocsPerSubQuery.get(index).scoreDocs[j].score = (1.f / (float) (rankConstant + j + 1));
                }
            }
        }
    }

}
