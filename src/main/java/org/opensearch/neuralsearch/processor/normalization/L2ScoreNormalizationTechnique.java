/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getNumOfSubqueries;

/**
 * Abstracts normalization of scores based on L2 method
 */
@ToString(onlyExplicitlyIncluded = true)
public class L2ScoreNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "l2";
    private static final float MIN_SCORE = 0.0f;

    public L2ScoreNormalizationTechnique() {
        this(Map.of(), new ScoreNormalizationUtil());
    }

    public L2ScoreNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        scoreNormalizationUtil.validateParameters(params, Set.of(), Map.of());
    }

    /**
     * L2 normalization method.
     * n_score_i = score_i/sqrt(score1^2 + score2^2 + ... + scoren^2)
     * Main algorithm steps:
     * - calculate sum of squares of all scores
     * - iterate over each result and update score as per formula above where "score" is raw score returned by Hybrid query
     */
    @Override
    public Map<Integer, float[]> normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        Map<Integer, float[]> docIdToSubqueryScores = new HashMap<>();
        List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        // get l2 norms for each sub-query
        List<Float> normsPerSubquery = getL2Norm(queryTopDocs);

        // do normalization using actual score and l2 norm
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    // Initialize or update subquery scores array per doc
                    float[] scoresArray = docIdToSubqueryScores.computeIfAbsent(scoreDoc.doc, k -> new float[topDocsPerSubQuery.size()]);
                    scoresArray[j] = scoreDoc.score;
                    scoreDoc.score = normalizeSingleScore(scoreDoc.score, normsPerSubquery.get(j));
                }
            }
        }
        return docIdToSubqueryScores;
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s", TECHNIQUE_NAME);
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(List<CompoundTopDocs> queryTopDocs) {
        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
        List<Float> normsPerSubquery = getL2Norm(queryTopDocs);

        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numberOfSubQueries = topDocsPerSubQuery.size();
            for (int subQueryIndex = 0; subQueryIndex < numberOfSubQueries; subQueryIndex++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(subQueryIndex);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(scoreDoc.doc, compoundQueryTopDocs.getSearchShard());
                    float normalizedScore = normalizeSingleScore(scoreDoc.score, normsPerSubquery.get(subQueryIndex));
                    ScoreNormalizationUtil.setNormalizedScore(
                        normalizedScores,
                        docIdAtSearchShard,
                        subQueryIndex,
                        numberOfSubQueries,
                        normalizedScore
                    );
                    scoreDoc.score = normalizedScore;
                }
            }
        }
        return getDocIdAtQueryForNormalization(normalizedScores, this);
    }

    private List<Float> getL2Norm(final List<CompoundTopDocs> queryTopDocs) {
        // find any non-empty compound top docs, it's either empty if shard does not have any results for all of sub-queries,
        // or it has results for all the sub-queries. In edge case of shard having results only for one sub-query, there will be TopDocs for
        // rest of sub-queries with zero total hits
        int numOfSubqueries = getNumOfSubqueries(queryTopDocs);
        float[] l2Norms = new float[numOfSubqueries];
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int bound = topDocsPerSubQuery.size();
            for (int index = 0; index < bound; index++) {
                for (ScoreDoc scoreDocs : topDocsPerSubQuery.get(index).scoreDocs) {
                    l2Norms[index] += scoreDocs.score * scoreDocs.score;
                }
            }
        }
        for (int index = 0; index < l2Norms.length; index++) {
            l2Norms[index] = (float) Math.sqrt(l2Norms[index]);
        }
        List<Float> l2NormList = new ArrayList<>();
        for (int index = 0; index < numOfSubqueries; index++) {
            l2NormList.add(l2Norms[index]);
        }
        return l2NormList;
    }

    private float normalizeSingleScore(final float score, final float l2Norm) {
        return l2Norm == 0 ? MIN_SCORE : score / l2Norm;
    }
}
