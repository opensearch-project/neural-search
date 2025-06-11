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
    private static final float SINGLE_RESULT_SCORE = 1.0f;
    private static final String PARAM_NAME_LOWER_BOUNDS = "lower_bounds";
    private static final String PARAM_NAME_LOWER_BOUND_MODE = "mode";
    private static final String PARAM_NAME_LOWER_BOUND_MIN_SCORE = "min_score";

    private static final Set<String> SUPPORTED_PARAMETERS = Set.of(PARAM_NAME_LOWER_BOUNDS);
    private static final Map<String, Set<String>> NESTED_PARAMETERS = Map.of(
        PARAM_NAME_LOWER_BOUNDS,
        Set.of(PARAM_NAME_LOWER_BOUND_MODE, PARAM_NAME_LOWER_BOUND_MIN_SCORE)
    );
    private final boolean subQueryScores;

    private final Optional<List<Pair<LowerBound.Mode, Float>>> lowerBoundsOptional;

    public MinMaxScoreNormalizationTechnique() {
        this(Map.of(), new ScoreNormalizationUtil(), false);
    }

    public MinMaxScoreNormalizationTechnique(
        final Map<String, Object> params,
        final ScoreNormalizationUtil scoreNormalizationUtil,
        final boolean subQueryScores
    ) {
        scoreNormalizationUtil.validateParameters(params, SUPPORTED_PARAMETERS, NESTED_PARAMETERS);
        lowerBoundsOptional = getLowerBounds(params);
        this.subQueryScores = subQueryScores;
    }

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     * Main algorithm steps:
     * - calculate min and max scores for each sub query
     * - iterate over each result and update score as per formula above where "score" is raw score returned by Hybrid query
     */
    @Override
    public Map<Integer, float[]> normalize(final NormalizeScoresDTO normalizeScoresDTO) {
        Map<Integer, float[]> docIdToSubqueryScores = new HashMap<>();
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);
        // do normalization using actual score and min and max scores for corresponding sub query
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            if (isLowerBoundsAndSubQueriesCountMismatched(topDocsPerSubQuery)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "expected lower bounds array to contain %d elements matching the number of sub-queries, but found a mismatch",
                        topDocsPerSubQuery.size()
                    )
                );
            }
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                LowerBound lowerBound = getLowerBound(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    // Initialize or update subquery scores array per doc
                    if (subQueryScores) {
                        float[] scoresArray = docIdToSubqueryScores.computeIfAbsent(
                            scoreDoc.doc,
                            k -> new float[topDocsPerSubQuery.size()]
                        );
                        scoresArray[j] = scoreDoc.score;
                    }
                    scoreDoc.score = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.getMinScoresPerSubquery()[j],
                        minMaxScores.getMaxScoresPerSubquery()[j],
                        lowerBound
                    );
                }
            }
        }

        return docIdToSubqueryScores;
    }

    private boolean isLowerBoundsAndSubQueriesCountMismatched(List<TopDocs> topDocsPerSubQuery) {
        return lowerBoundsOptional.isPresent()
            && topDocsPerSubQuery.isEmpty() == false
            && lowerBoundsOptional.get().size() != topDocsPerSubQuery.size();
    }

    private LowerBound getLowerBound(int subQueryIndex) {
        return lowerBoundsOptional.map(
            pairs -> new LowerBound(true, pairs.get(subQueryIndex).getLeft(), pairs.get(subQueryIndex).getRight())
        ).orElseGet(LowerBound::new);
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
        return lowerBoundsOptional.map(lb -> {
            String lowerBounds = lb.stream()
                .map(pair -> String.format(Locale.ROOT, "(%s, %s)", pair.getLeft(), pair.getRight()))
                .collect(Collectors.joining(", ", "[", "]"));
            return String.format(Locale.ROOT, "%s, lower bounds %s", TECHNIQUE_NAME, lowerBounds);
        }).orElse(String.format(Locale.ROOT, "%s", TECHNIQUE_NAME));
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

    private float normalizeSingleScore(final float score, final float minScore, final float maxScore, final LowerBound lowerBound) {
        // edge case when there is only one score and min and max scores are same
        if (Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        if (lowerBound.isEnabled() == false) {
            return LowerBound.Mode.IGNORE.normalize(score, minScore, maxScore, lowerBound.getMinScore());
        }
        return lowerBound.getMode().normalize(score, minScore, maxScore, lowerBound.getMinScore());
    }

    /**
     * Get lower bounds from input parameters
     * @param params user provided input parameters for this technique
     * @return optional list of lower bounds. Can be empty in case lower bounds are not provided
     */
    private Optional<List<Pair<LowerBound.Mode, Float>>> getLowerBounds(final Map<String, Object> params) {
        // validate that the input parameters are in correct format
        if (Objects.isNull(params) || params.containsKey(PARAM_NAME_LOWER_BOUNDS) == false) {
            return Optional.empty();
        }

        List<?> lowerBoundsParams = Optional.ofNullable(params.get(PARAM_NAME_LOWER_BOUNDS))
            .filter(List.class::isInstance)
            .map(List.class::cast)
            .orElseThrow(() -> new IllegalArgumentException("lower_bounds must be a List"));
        // number of lower bounds must match the number of sub-queries in a hybrid query
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
        // parse each lower bound item and put all items in a list
        List<Pair<LowerBound.Mode, Float>> lowerBounds = lowerBoundsParams.stream().map(this::parseLowerBound).collect(Collectors.toList());

        return Optional.of(lowerBounds);
    }

    @SuppressWarnings("unchecked")
    /**
     * Parse each lower bound item and return a pair of mode and min score
     * @param boundObj lower bound item provided by the client
     * @return a single pair of mode and min score
     */
    private Pair<LowerBound.Mode, Float> parseLowerBound(Object boundObj) {
        if ((boundObj instanceof Map) == false) {
            throw new IllegalArgumentException("each lower bound must be a map");
        }

        Map<String, Object> lowerBound = (Map<String, Object>) boundObj;

        String lowerBoundModeValue = Objects.toString(lowerBound.get(PARAM_NAME_LOWER_BOUND_MODE), "");
        LowerBound.Mode mode = LowerBound.Mode.fromString(lowerBoundModeValue);
        float minScore = extractAndValidateMinScore(lowerBound);

        return ImmutablePair.of(mode, minScore);
    }

    private float extractAndValidateMinScore(Map<String, Object> lowerBound) {
        Object minScoreObj = lowerBound.get(PARAM_NAME_LOWER_BOUND_MIN_SCORE);
        if (minScoreObj == null) {
            return LowerBound.DEFAULT_LOWER_BOUND_SCORE;
        }
        try {
            float minScore = LowerBound.DEFAULT_LOWER_BOUND_SCORE;
            if (Objects.nonNull(lowerBound.get(PARAM_NAME_LOWER_BOUND_MIN_SCORE))) {
                minScore = Float.parseFloat(String.valueOf(lowerBound.get(PARAM_NAME_LOWER_BOUND_MIN_SCORE)));
            }
            Validate.isTrue(
                minScore >= LowerBound.MIN_LOWER_BOUND_SCORE && minScore <= LowerBound.MAX_LOWER_BOUND_SCORE,
                "min_score must be a valid finite number between %f and %f",
                LowerBound.MIN_LOWER_BOUND_SCORE,
                LowerBound.MAX_LOWER_BOUND_SCORE
            );
            return minScore;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid format for min_score: must be a valid float value", e);
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

    /**
     * Result class to hold lower bound for each sub query
     */
    @Getter
    static class LowerBound {
        static final float MIN_LOWER_BOUND_SCORE = -10_000f;
        static final float MAX_LOWER_BOUND_SCORE = 10_000f;
        static final float DEFAULT_LOWER_BOUND_SCORE = 0.0f;

        private final boolean enabled;
        private final Mode mode;
        private final float minScore;

        LowerBound() {
            this(false, Mode.DEFAULT, DEFAULT_LOWER_BOUND_SCORE);
        }

        LowerBound(boolean enabled, Mode mode, float minScore) {
            this.enabled = enabled;
            this.mode = mode;
            this.minScore = minScore;
        }

        /**
         * Enum for normalization mode
         */
        protected enum Mode {
            APPLY {
                @Override
                public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                    // if we apply the lower bound this mean we use actual score in case it's less then the lower bound min score
                    // same applied to case when actual max_score is less than lower bound min score
                    if (maxScore < lowerBoundScore || score < lowerBoundScore) {
                        return (score - minScore) / (maxScore - minScore);
                    }
                    return (score - lowerBoundScore) / (maxScore - lowerBoundScore);
                }
            },
            CLIP {
                @Override
                public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                    // apply clipping, return lower bound min score if score is less than min score. This effectively means 0 after
                    // normalization
                    if (score < minScore) {
                        return 0.0f;
                    }
                    if (maxScore < lowerBoundScore) {
                        return (score - minScore) / (maxScore - minScore);
                    }
                    return (score - lowerBoundScore) / (maxScore - lowerBoundScore);
                }
            },
            IGNORE {
                @Override
                public float normalize(float score, float minScore, float maxScore, float lowerBoundScore) {
                    // ignore lower bound logic and do raw min-max normalization using actual scores
                    float normalizedScore = (score - minScore) / (maxScore - minScore);
                    return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
                }
            };

            public static final Mode DEFAULT = APPLY;
            // set of all valid values for mode
            public static final String VALID_VALUES = Arrays.stream(values())
                .map(mode -> mode.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(", "));

            /**
             * Get mode from string value
             * @param value string value of mode
             * @return mode
             * @throws IllegalArgumentException if mode is not valid
             */
            public static Mode fromString(String value) {
                if (Objects.isNull(value)) {
                    throw new IllegalArgumentException("mode value cannot be null or empty");
                }
                if (value.trim().isEmpty()) {
                    return DEFAULT;
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

            @Override
            public String toString() {
                return name().toLowerCase(Locale.ROOT);
            }
        }
    }
}
