/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;
import static org.opensearch.neuralsearch.query.HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

/**
 * Abstracts normalization of scores based on min-max method
 */
@ToString(onlyExplicitlyIncluded = true)
public class MinMaxScoreNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "min_max";
    protected static final float MIN_SCORE = 0.001f;
    private static final float SINGLE_RESULT_SCORE = 1.0f;
    private final List<Pair<Mode, Float>> lowerBounds;

    public MinMaxScoreNormalizationTechnique() {
        this(Map.of());
    }

    public MinMaxScoreNormalizationTechnique(final Map<String, Object> params) {
        lowerBounds = getLowerBounds(params);
    }

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     * Main algorithm steps:
     * - calculate min and max scores for each sub query
     * - iterate over each result and update score as per formula above where "score" is raw score returned by Hybrid query
     */
    @Override
    public void normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);
        // do normalization using actual score and min and max scores for corresponding sub query
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            if (Objects.nonNull(lowerBounds) && !lowerBounds.isEmpty() && lowerBounds.size() != topDocsPerSubQuery.size()) {
                throw new IllegalArgumentException("lower bounds size should be same as number of sub queries");
            }
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                LowerBound lowerBound = getLowerBound(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[j],
                        minMaxScores.getMaxScoresPerSubquery()[j],
                        lowerBound
                    );
                }
            }
        }
    }

    private LowerBound getLowerBound(int j) {
        LowerBound lowerBound;
        if (Objects.isNull(lowerBounds) || lowerBounds.isEmpty()) {
            lowerBound = new LowerBound();
        } else {
            lowerBound = new LowerBound(true, lowerBounds.get(j).getLeft(), lowerBounds.get(j).getRight());
        }
        return lowerBound;
    }

    private MinMaxScores getMinMaxScoresResult(final List<CompoundTopDocs> queryTopDocs) {
        int numOfSubqueries = getNumOfSubqueries(queryTopDocs);
        // get min scores for each sub query
        float[] minScoresPerSubquery = getMinScores(queryTopDocs, numOfSubqueries);
        // get max scores for each sub query
        float[] maxScoresPerSubquery = getMaxScores(queryTopDocs, numOfSubqueries);
        return new MinMaxScores(minScoresPerSubquery, maxScoresPerSubquery);
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s", TECHNIQUE_NAME);
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(final List<CompoundTopDocs> queryTopDocs) {
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);

        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
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
                    LowerBound lowerBound = getLowerBound(subQueryIndex);
                    float normalizedScore = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[subQueryIndex],
                        minMaxScores.getMaxScoresPerSubquery()[subQueryIndex],
                        lowerBound
                    );
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

    private int getNumOfSubqueries(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> !topDocs.getTopDocs().isEmpty())
            .findAny()
            .get()
            .getTopDocs()
            .size();
    }

    private float[] getMaxScores(final List<CompoundTopDocs> queryTopDocs, final int numOfSubqueries) {
        float[] maxScores = new float[numOfSubqueries];
        Arrays.fill(maxScores, Float.MIN_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                maxScores[j] = Math.max(
                    maxScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .max(Float::compare)
                        .orElse(Float.MIN_VALUE)
                );
            }
        }
        return maxScores;
    }

    private float[] getMinScores(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        float[] minScores = new float[numOfScores];
        Arrays.fill(minScores, Float.MAX_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                // LowerBound lowerBound = getLowerBound(j);
                // we need to compute actual min score for everything except clipping. For clipping we have to use
                // lower bound min_score, it's passed as parameter. If we skip for clipping we can save some CPU cycles.
                // if (!lowerBound.isEnabled() || lowerBound.getMode() != Mode.CLIP) {
                minScores[j] = Math.min(
                    minScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .min(Float::compare)
                        .orElse(Float.MAX_VALUE)
                );
                // }
            }
        }
        return minScores;
    }

    private float normalizeSingleScore(final float score, final float minScore, final float maxScore, LowerBound lowerBound) {
        // edge case when there is only one score and min and max scores are same
        if (Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        if (!lowerBound.isEnabled()) {
            return Mode.IGNORE.normalize(score, minScore, maxScore, lowerBound.getMinScore());
        }

        return lowerBound.getMode().normalize(score, minScore, maxScore, lowerBound.getMinScore());
    }

    /**
     * Result class to hold min and max scores for each sub query
     */
    @AllArgsConstructor
    @Getter
    private class MinMaxScores {
        float[] minScoresPerSubquery;
        float[] maxScoresPerSubquery;
    }

    private List<Pair<Mode, Float>> getLowerBounds(final Map<String, Object> params) {
        List<Pair<Mode, Float>> lowerBounds = new ArrayList<>();

        // Early return if params is null or doesn't contain lower_bounds
        if (Objects.isNull(params) || !params.containsKey("lower_bounds")) {
            return lowerBounds;
        }

        Object lowerBoundsObj = params.get("lower_bounds");
        if (!(lowerBoundsObj instanceof List<?> lowerBoundsParams)) {
            throw new IllegalArgumentException("lower_bounds must be a List");
        }

        if (lowerBoundsParams.size() > MAX_NUMBER_OF_SUB_QUERIES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "lower_bounds size %d should be less than or equal to %d",
                    lowerBoundsParams.size(),
                    MAX_NUMBER_OF_SUB_QUERIES
                )
            );
        }

        for (Object boundObj : lowerBoundsParams) {
            if (!(boundObj instanceof Map)) {
                throw new IllegalArgumentException("each lower bound must be a map");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> lowerBound = (Map<String, Object>) boundObj;

            try {
                Mode mode = Mode.fromString(lowerBound.get("mode").toString());
                float minScore = Float.parseFloat(String.valueOf(lowerBound.get("min_score")));

                Validate.isTrue(
                    minScore >= LowerBound.MIN_LOWER_BOUND_SCORE && minScore <= LowerBound.MAX_LOWER_BOUND_SCORE,
                    "min_score must be a valid finite number between %f and %f",
                    LowerBound.MIN_LOWER_BOUND_SCORE,
                    LowerBound.MAX_LOWER_BOUND_SCORE
                );

                lowerBounds.add(ImmutablePair.of(mode, minScore));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid format for min_score: must be a valid float value", e);
            }
        }

        return lowerBounds;
    }

    /**
     * Result class to hold lower bound for each sub query
     */
    @Getter
    private static class LowerBound {
        static final float MIN_LOWER_BOUND_SCORE = -10_000f;
        static final float MAX_LOWER_BOUND_SCORE = 10_000f;
        static final float DEFAULT_LOWER_BOUND_SCORE = 0.0f;

        boolean enabled;
        Mode mode;
        float minScore;

        LowerBound() {
            this(false, Mode.DEFAULT, DEFAULT_LOWER_BOUND_SCORE);
        }

        LowerBound(boolean enabled, Mode mode, float minScore) {
            this.enabled = enabled;
            this.mode = mode;
            this.minScore = minScore;
        }
    }

    protected enum Mode {
        APPLY {
            @Override
            public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                if (maxScore < lowerBoundScore) {
                    return (score - minScore) / (maxScore - minScore);
                } else if (score < lowerBoundScore) {
                    return score / (maxScore - score);
                }
                return (score - lowerBoundScore) / (maxScore - lowerBoundScore);
            }
        },
        CLIP {
            @Override
            public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                if (score < minScore) {
                    return lowerBoundScore / (maxScore - lowerBoundScore);
                }
                return (score - lowerBoundScore) / (maxScore - lowerBoundScore);
            }
        },
        IGNORE {
            @Override
            public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                float normalizedScore = (score - minScore) / (maxScore - minScore);
                return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
            }
        };

        public static final Mode DEFAULT = APPLY;
        public static final String VALID_VALUES = Arrays.stream(values())
            .map(mode -> mode.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.joining(", "));

        public static Mode fromString(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("mode value cannot be null or empty");
            }

            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid mode: %s, valid values are: %s", value, VALID_VALUES)
                );
            }
        }

        public abstract float normalize(float score, float minScore, float maxScore, float lowerBoundScore);
    }
}
