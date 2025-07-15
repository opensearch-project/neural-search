/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.ml.repackage.com.google.common.annotations.VisibleForTesting;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.normalization.bounds.BoundMode;
import org.opensearch.neuralsearch.processor.normalization.bounds.LowerBound;
import org.opensearch.neuralsearch.processor.normalization.bounds.UpperBound;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;
import static org.opensearch.neuralsearch.processor.normalization.bounds.ScoreBound.MAX_BOUND_SCORE;
import static org.opensearch.neuralsearch.processor.normalization.bounds.ScoreBound.MIN_BOUND_SCORE;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getNumOfSubqueries;
import static org.opensearch.neuralsearch.query.HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES;

/**
 * Abstracts normalization of scores based on min-max method
 */
@ToString(onlyExplicitlyIncluded = true)
public class MinMaxScoreNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "min_max";
    protected static final float MIN_SCORE = 0.001f;
    protected static final float MAX_SCORE = 1.0f;
    private static final float SINGLE_RESULT_SCORE = 1.0f;
    private static final String PARAM_NAME_LOWER_BOUNDS = "lower_bounds";
    private static final String PARAM_NAME_BOUND_MODE = "mode";
    private static final String PARAM_NAME_LOWER_BOUND_MIN_SCORE = "min_score";
    private static final String PARAM_NAME_UPPER_BOUNDS = "upper_bounds";
    private static final String PARAM_NAME_UPPER_BOUND_MAX_SCORE = "max_score";

    private static final Set<String> SUPPORTED_PARAMETERS = Set.of(PARAM_NAME_LOWER_BOUNDS, PARAM_NAME_UPPER_BOUNDS);
    private static final Map<String, Set<String>> NESTED_PARAMETERS = Map.of(
        PARAM_NAME_LOWER_BOUNDS,
        Set.of(PARAM_NAME_BOUND_MODE, PARAM_NAME_LOWER_BOUND_MIN_SCORE),
        PARAM_NAME_UPPER_BOUNDS,
        Set.of(PARAM_NAME_BOUND_MODE, PARAM_NAME_UPPER_BOUND_MAX_SCORE)
    );

    private final Optional<List<Map<String, Object>>> lowerBoundsParamsOptional;
    private final Optional<List<Map<String, Object>>> upperBoundsParamsOptional;

    public MinMaxScoreNormalizationTechnique() {
        this(Map.of(), new ScoreNormalizationUtil());
    }

    public MinMaxScoreNormalizationTechnique(final Map<String, Object> params, final ScoreNormalizationUtil scoreNormalizationUtil) {
        scoreNormalizationUtil.validateParameters(params, SUPPORTED_PARAMETERS, NESTED_PARAMETERS);
        lowerBoundsParamsOptional = getBoundsParams(params, PARAM_NAME_LOWER_BOUNDS);
        upperBoundsParamsOptional = getBoundsParams(params, PARAM_NAME_UPPER_BOUNDS);
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
            if (isBoundsAndSubQueriesCountMismatched(topDocsPerSubQuery)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "expected bounds array to contain %d elements matching the number of sub-queries, but found a mismatch",
                        topDocsPerSubQuery.size()
                    )
                );
            }
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                LowerBound lowerBound = getLowerBound(j);
                UpperBound upperBound = getUpperBound(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[j],
                        minMaxScores.getMaxScoresPerSubquery()[j],
                        lowerBound,
                        upperBound
                    );
                }
            }
        }
    }

    private boolean isBoundsAndSubQueriesCountMismatched(List<TopDocs> topDocsPerSubQuery) {
        boolean lowerBoundsMismatch = lowerBoundsParamsOptional.isPresent()
            && !topDocsPerSubQuery.isEmpty()
            && lowerBoundsParamsOptional.get().size() != topDocsPerSubQuery.size();
        boolean upperBoundsMismatch = upperBoundsParamsOptional.isPresent()
            && !topDocsPerSubQuery.isEmpty()
            && upperBoundsParamsOptional.get().size() != topDocsPerSubQuery.size();
        return lowerBoundsMismatch || upperBoundsMismatch;
    }

    private LowerBound getLowerBound(int subQueryIndex) {
        return lowerBoundsParamsOptional.map(bounds -> bounds.get(subQueryIndex)).map(LowerBound::new).orElseGet(LowerBound::new);
    }

    private UpperBound getUpperBound(int subQueryIndex) {
        return upperBoundsParamsOptional.map(bounds -> bounds.get(subQueryIndex)).map(UpperBound::new).orElseGet(UpperBound::new);
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
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        StringBuilder description = new StringBuilder(TECHNIQUE_NAME);

        lowerBoundsParamsOptional.ifPresent(lb -> {
            String lowerBounds = lb.stream().map(boundMap -> {
                BoundMode mode = BoundMode.fromString(Objects.toString(boundMap.get("mode"), ""));
                String minScore = Objects.toString(boundMap.get("min_score"), String.valueOf(LowerBound.DEFAULT_LOWER_BOUND_SCORE));
                return String.format(Locale.ROOT, "(%s, %s)", mode, minScore);
            }).collect(Collectors.joining(", ", "[", "]"));
            description.append(String.format(Locale.ROOT, ", lower bounds %s", lowerBounds));
        });

        upperBoundsParamsOptional.ifPresent(ub -> {
            String upperBounds = ub.stream().map(boundMap -> {
                BoundMode mode = BoundMode.fromString(Objects.toString(boundMap.get("mode"), ""));
                String maxScore = Objects.toString(boundMap.get("max_score"), String.valueOf(UpperBound.DEFAULT_UPPER_BOUND_SCORE));
                return String.format(Locale.ROOT, "(%s, %s)", mode, maxScore);
            }).collect(Collectors.joining(", ", "[", "]"));
            description.append(String.format(Locale.ROOT, ", upper bounds %s", upperBounds));
        });

        return description.toString();
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
                    UpperBound upperBound = getUpperBound(subQueryIndex);
                    float normalizedScore = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[subQueryIndex],
                        minMaxScores.getMaxScoresPerSubquery()[subQueryIndex],
                        lowerBound,
                        upperBound
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
                minScores[j] = Math.min(
                    minScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .min(Float::compare)
                        .orElse(Float.MAX_VALUE)
                );
            }
        }
        return minScores;
    }

    private float normalizeSingleScore(
        final float score,
        final float minScore,
        final float maxScore,
        final LowerBound lowerBound,
        final UpperBound upperBound
    ) {
        // edge case when there is only one score and min and max scores are same
        if (isSingleScore(score, minScore, maxScore)) {
            return SINGLE_RESULT_SCORE;
        }

        float effectiveMinScore = lowerBound.determineEffectiveScore(score, minScore, maxScore);
        float effectiveMaxScore = upperBound.determineEffectiveScore(score, minScore, maxScore);

        if (lowerBound.shouldClipToBound(score, effectiveMinScore)) {
            return MIN_SCORE;
        }
        if (upperBound.shouldClipToBound(score, effectiveMaxScore)) {
            return MAX_SCORE;
        }

        return calculateNormalizedScore(score, effectiveMinScore, effectiveMaxScore);
    }

    private boolean isSingleScore(float score, float minScore, float maxScore) {
        return Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0;
    }

    @VisibleForTesting
    protected float calculateNormalizedScore(float score, float effectiveMinScore, float effectiveMaxScore) {
        if (Floats.compare(effectiveMaxScore, effectiveMinScore) == 0) {
            return SINGLE_RESULT_SCORE;
        }

        float normalizedScore = (score - effectiveMinScore) / (effectiveMaxScore - effectiveMinScore);
        return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
    }

    private Optional<List<Map<String, Object>>> getBoundsParams(final Map<String, Object> params, String paramName) {
        if (Objects.isNull(params) || !params.containsKey(paramName)) {
            return Optional.empty();
        }

        List<?> boundsParams = Optional.ofNullable(params.get(paramName))
            .filter(List.class::isInstance)
            .map(List.class::cast)
            .orElseThrow(() -> new IllegalArgumentException(paramName + " must be a List"));

        if (boundsParams.size() > MAX_NUMBER_OF_SUB_QUERIES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s size %d should be less than or equal to %d",
                    paramName,
                    boundsParams.size(),
                    MAX_NUMBER_OF_SUB_QUERIES
                )
            );
        }

        String scoreParamName;
        float defaultScore;
        switch (paramName) {
            case PARAM_NAME_LOWER_BOUNDS:
                scoreParamName = PARAM_NAME_LOWER_BOUND_MIN_SCORE;
                defaultScore = LowerBound.DEFAULT_LOWER_BOUND_SCORE;
                break;
            case PARAM_NAME_UPPER_BOUNDS:
                scoreParamName = PARAM_NAME_UPPER_BOUND_MAX_SCORE;
                defaultScore = UpperBound.DEFAULT_UPPER_BOUND_SCORE;
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Unsupported bounds parameter name: %s", paramName));
        }

        return Optional.of(boundsParams.stream().map(item -> {
            if (!(item instanceof Map)) {
                throw new IllegalArgumentException("each bound must be a map");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> boundMap = (Map<String, Object>) item;

            // Validate mode
            String modeString = Objects.toString(boundMap.get(PARAM_NAME_BOUND_MODE), "");
            if (!modeString.isEmpty()) {
                BoundMode.fromString(modeString);
            }

            // Validate score
            validateBoundScore(boundMap, scoreParamName, defaultScore);

            return boundMap;
        }).collect(Collectors.toList()));
    }

    private void validateBoundScore(Map<String, Object> bound, String scoreParamName, float defaultScore) {
        Object scoreObj = bound.get(scoreParamName);
        if (scoreObj == null) {
            return;
        }
        try {
            float score = Float.parseFloat(String.valueOf(scoreObj));
            Validate.isTrue(
                score >= MIN_BOUND_SCORE && score <= MAX_BOUND_SCORE,
                "%s must be a valid finite number between %f and %f",
                scoreParamName,
                MIN_BOUND_SCORE,
                MAX_BOUND_SCORE
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "invalid format for %s: must be a valid float value", scoreParamName),
                e
            );
        }
    }

    /**
     * Result class to hold min and max scores for each sub query
     */
    @AllArgsConstructor
    @Getter
    private static class MinMaxScores {
        float[] minScoresPerSubquery;
        float[] maxScoresPerSubquery;
    }
}
