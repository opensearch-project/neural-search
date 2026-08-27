/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;

/**
 * Immutable, resolved fusion configuration for the {@code hybrid} query resolver (fused) mode — the normalization +
 * combination technique, per-leg weights, and RRF rank_constant. The resolver is enabled by the single {@code fusion}
 * parameter on the query; its config comes from one of two interchangeable sources, both interpreted by this class:
 *
 * <ul>
 *   <li>an inline {@code fusion} block on the query body ({@link #fromInlineFusion(Map)}), or</li>
 *   <li>the attached search pipeline's phase-results processor read at coordinator rewrite
 *       ({@link #fromPipelineConfig(Map)}) — the same config source as classic hybrid, giving zero-migration UX.</li>
 * </ul>
 *
 * <p>Two processor shapes are understood, mirroring the classic phase-results processors. Which one a config came from
 * is retained as its {@link Shape}, because the resolved technique names do not always distinguish them:
 * <ul>
 *   <li>{@code normalization-processor}: {@code normalization.technique} (min_max|l2|z_score|rrf) + optional
 *       {@code normalization.parameters} (rank_constant, for rrf) + {@code combination.technique} (arithmetic_mean) +
 *       optional {@code combination.parameters.weights}.</li>
 *   <li>{@code score-ranker-processor}: {@code combination.technique = rrf} + {@code combination.rank_constant} (on the
 *       combination clause itself, NOT under {@code parameters} — that is where {@code RRFProcessorFactory} reads it) +
 *       optional {@code combination.parameters.weights}. RRF is rank-based, so this shape carries no normalization
 *       clause; it still resolves to {@code normalization = rrf}, because that is the normalization classic applies for
 *       it — see {@link #readNormalizationTechnique}.</li>
 * </ul>
 *
 * <p>Note that {@code rrf} appears in both, and means different things: under a {@code normalization-processor} it is
 * only the normalization step and one of the means does the combining, while the {@code score-ranker-processor} is
 * reciprocal rank fusion end to end. That is also why {@code rank_constant} is read from a different place in each —
 * each shape reads it where its own classic factory reads it.
 */
@Getter(AccessLevel.PACKAGE)
@Accessors(fluent = true)
public final class FusionSpec {

    /**
     * Which of the two processor shapes a config was read as. Carried rather than re-derived because the resolved
     * technique names alone cannot tell {@code normalization = rrf, combination = rrf} apart, and only one of the two
     * shapes may legitimately produce it: reciprocal rank fusion is the {@code score-ranker-processor}'s whole job,
     * whereas a {@code normalization-processor} combining rrf-normalized scores by rrf is a pairing classic's
     * compatibility matrix rejects. The fused-mode gate keys its one exemption from that matrix on this.
     */
    enum Shape {
        /** {@code normalization-processor}: a normalization technique whose scores one of the means combines. */
        NORMALIZATION_PROCESSOR,
        /** {@code score-ranker-processor}: reciprocal rank fusion supplying both normalization and combination. */
        SCORE_RANKER_PROCESSOR
    }

    // Combination techniques
    static final String TECHNIQUE_RRF = RRFScoreCombinationTechnique.TECHNIQUE_NAME;
    static final String TECHNIQUE_ARITHMETIC_MEAN = ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME;
    // Normalization techniques
    static final String NORMALIZATION_MIN_MAX = MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME;
    static final String NORMALIZATION_RRF = RRFNormalizationTechnique.TECHNIQUE_NAME;

    // Sourced from the shared normalizer rather than redeclared, so fused mode and classic cannot drift apart.
    static final int DEFAULT_RANK_CONSTANT = RRFScoreNormalizer.DEFAULT_RANK_CONSTANT;

    // Config-map keys (shared by the normalization-processor and score-ranker-processor definitions)
    private static final String PHASE_RESULTS_PROCESSORS_KEY = "phase_results_processors";
    private static final String NORMALIZATION_CLAUSE = "normalization";
    private static final String COMBINATION_CLAUSE = "combination";
    private static final String TECHNIQUE_KEY = "technique";
    private static final String PARAMETERS_KEY = "parameters";
    private static final String WEIGHTS_KEY = "weights";
    private static final String RANK_CONSTANT_KEY = RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT;

    private final Shape shape; // which processor shape this was read as
    private final String combinationTechnique; // rrf | arithmetic_mean
    private final String normalizationTechnique; // min_max | z_score | l2 | rrf
    private final int rankConstant; // RRF only
    private final float[] weights; // per-leg weights; empty => unweighted

    FusionSpec(Shape shape, String combinationTechnique, String normalizationTechnique, int rankConstant, float[] weights) {
        this.shape = Objects.requireNonNull(shape);
        this.combinationTechnique = combinationTechnique;
        // Both factories always resolve a name, each shape defaulting its own way, so there is no null case here — and
        // nothing sensible to default it to, now that "none" is not a technique fused mode understands.
        this.normalizationTechnique = Objects.requireNonNull(normalizationTechnique);
        this.rankConstant = rankConstant;
        this.weights = Objects.isNull(weights) ? new float[0] : weights;
    }

    /**
     * Read a {@link FusionSpec} from a resolved search-pipeline config map (the shape returned by
     * {@code PipelineConfiguration.getConfigAsMap()} or an inline {@code search_pipeline} body block). Walks
     * {@code phase_results_processors} for the first {@code normalization-processor} or {@code score-ranker-processor}
     * entry and reads its technique/weights. Returns {@code null} when the pipeline has no fusion processor — the
     * caller (fused-mode doRewrite) then fails fast rather than emitting unfused scores.
     *
     * @param pipelineConfig the resolved pipeline config map (never null)
     * @return the parsed fusion spec, or null when no normalization/score-ranker processor is present
     */
    @SuppressWarnings("unchecked")
    static FusionSpec fromPipelineConfig(Map<String, Object> pipelineConfig) {
        if (Objects.isNull(pipelineConfig)) {
            return null;
        }
        Object phaseProcessors = pipelineConfig.get(PHASE_RESULTS_PROCESSORS_KEY);
        if ((phaseProcessors instanceof List) == false) {
            return null;
        }
        for (Object processorEntry : (List<Object>) phaseProcessors) {
            if ((processorEntry instanceof Map) == false) {
                continue;
            }
            Map<String, Object> processorMap = (Map<String, Object>) processorEntry;
            if (processorMap.get(NormalizationProcessor.TYPE) instanceof Map) {
                return fromNormalizationProcessor((Map<String, Object>) processorMap.get(NormalizationProcessor.TYPE));
            }
            if (processorMap.get(RRFProcessor.TYPE) instanceof Map) {
                return fromScoreRankerProcessor((Map<String, Object>) processorMap.get(RRFProcessor.TYPE));
            }
        }
        return null;
    }

    /**
     * Read a {@link FusionSpec} from an inline {@code fusion} block on the query body (precedence step 1: inline wins
     * over the attached pipeline). The block mirrors the processor JSON verbatim —
     * {@code {normalization: {technique}, combination: {technique, rank_constant, parameters: {weights}}}} — so this
     * reuses the pipeline-config parsing. {@code combination.technique: rrf} routes to the rank-constant shape.
     *
     * @param fusionConfig the parsed inline fusion map (nullable)
     * @return the parsed fusion spec, or null when the map is null
     */
    @SuppressWarnings("unchecked")
    static FusionSpec fromInlineFusion(Map<String, Object> fusionConfig) {
        if (Objects.isNull(fusionConfig)) {
            return null;
        }
        if (fusionConfig.get(COMBINATION_CLAUSE) instanceof Map) {
            Object technique = ((Map<String, Object>) fusionConfig.get(COMBINATION_CLAUSE)).get(TECHNIQUE_KEY);
            if (Objects.nonNull(technique) && TECHNIQUE_RRF.equals(technique.toString().toLowerCase(Locale.ROOT))) {
                return fromScoreRankerProcessor(fusionConfig);
            }
        }
        return fromNormalizationProcessor(fusionConfig);
    }

    @SuppressWarnings("unchecked")
    private static FusionSpec fromNormalizationProcessor(Map<String, Object> config) {
        String normalization = readNormalizationTechnique(config, NORMALIZATION_MIN_MAX);
        String combination = TECHNIQUE_ARITHMETIC_MEAN;
        float[] weights = new float[0];
        if (config.get(COMBINATION_CLAUSE) instanceof Map) {
            Map<String, Object> combinationClause = (Map<String, Object>) config.get(COMBINATION_CLAUSE);
            Object technique = combinationClause.get(TECHNIQUE_KEY);
            if (Objects.nonNull(technique)) {
                combination = technique.toString().toLowerCase(Locale.ROOT);
            }
            weights = readWeights(combinationClause);
        }
        return new FusionSpec(Shape.NORMALIZATION_PROCESSOR, combination, normalization, readRankConstant(config, normalization), weights);
    }

    /**
     * {@code rank_constant} for the {@code normalization-processor} shape, where {@code rrf} is the <em>normalization</em>
     * technique and its parameters therefore live under {@code normalization.parameters} — that is the map
     * {@code NormalizationProcessorFactory} hands to {@code RRFNormalizationTechnique}, so it is where fused mode has to
     * look. Reading it only off the combination clause (the other shape's location) left a rrf-normalized
     * {@code normalization-processor} pipeline ranking at the default 60 no matter what the user configured.
     *
     * <p>Only read for {@code rrf}, because it is an rrf parameter: resolving it for a score-based technique would answer
     * a stray {@code rank_constant} under {@code min_max} with a rank-constant range error instead of the unsupported-
     * parameter error {@code ScoreNormalizationUtil} raises for it.
     */
    private static int readRankConstant(Map<String, Object> config, String normalization) {
        if (NORMALIZATION_RRF.equals(normalization) == false) {
            return DEFAULT_RANK_CONSTANT;
        }
        // Shared resolver, so an absent value defaults and an out-of-range or non-integer one is rejected with the same
        // message classic gives, rather than silently falling back.
        return RRFScoreNormalizer.resolveRankConstant(readNormalizationParameters(config));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readNormalizationParameters(Map<String, Object> config) {
        if ((config.get(NORMALIZATION_CLAUSE) instanceof Map) == false) {
            return Map.of();
        }
        Map<String, Object> normalizationClause = (Map<String, Object>) config.get(NORMALIZATION_CLAUSE);
        return normalizationClause.get(PARAMETERS_KEY) instanceof Map
            ? (Map<String, Object>) normalizationClause.get(PARAMETERS_KEY)
            : Map.of();
    }

    @SuppressWarnings("unchecked")
    private static FusionSpec fromScoreRankerProcessor(Map<String, Object> config) {
        int rankConstant = DEFAULT_RANK_CONSTANT;
        float[] weights = new float[0];
        if (config.get(COMBINATION_CLAUSE) instanceof Map) {
            Map<String, Object> combinationClause = (Map<String, Object>) config.get(COMBINATION_CLAUSE);
            rejectRankConstantUnderParameters(combinationClause);
            // rank_constant sits on the combination clause itself, which is where RRFProcessorFactory reads it — NOT
            // under `parameters`, whose only supported key is `weights`. Delegating to the shared resolver makes fused
            // mode accept, reject and report exactly what classic does: absent -> default, non-integer -> "must be an
            // integer", outside [1, 10000] -> the range error. Reading the value directly here would silently accept a
            // negative, oversized or fractional rank constant that the score-ranker-processor rejects.
            rankConstant = RRFScoreNormalizer.resolveRankConstant(combinationClause);
            weights = readWeights(combinationClause);
        }
        // The score-ranker-processor has no normalization clause, so the default is what this shape almost always
        // resolves to. It is "rrf", not "none", because rank scoring IS this shape's normalization step — classic says so
        // itself: RRFProcessorFactory builds an RRFNormalizationTechnique for a processor with no normalization clause.
        // Naming it lets the coordinator resolve rrf through the same ScalarNormalizers lookup as every other technique.
        // An inline fusion block can still carry a normalization clause, and it is reported rather than dropped so the
        // caller's technique check rejects the contradictory pairing instead of silently ignoring what the user asked for.
        return new FusionSpec(
            Shape.SCORE_RANKER_PROCESSOR,
            TECHNIQUE_RRF,
            readNormalizationTechnique(config, NORMALIZATION_RRF),
            rankConstant,
            weights
        );
    }

    /**
     * {@code rank_constant} under {@code combination.parameters} is a config error, not a place we also look: the
     * score-ranker-processor rejects it there ("supported parameters are [weights]"). Fused mode rejects it too, rather
     * than silently falling back to the default 60 and mis-ranking every query for a user who put it in the wrong place.
     */
    @SuppressWarnings("unchecked")
    private static void rejectRankConstantUnderParameters(Map<String, Object> combinationClause) {
        if ((combinationClause.get(PARAMETERS_KEY) instanceof Map) == false) {
            return;
        }
        if (((Map<String, Object>) combinationClause.get(PARAMETERS_KEY)).containsKey(RANK_CONSTANT_KEY)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] must be set on the [%s] clause, not under [%s]; supported parameters are [%s]",
                    RANK_CONSTANT_KEY,
                    COMBINATION_CLAUSE,
                    PARAMETERS_KEY,
                    WEIGHTS_KEY
                )
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static String readNormalizationTechnique(Map<String, Object> config, String defaultTechnique) {
        if ((config.get(NORMALIZATION_CLAUSE) instanceof Map) == false) {
            return defaultTechnique;
        }
        Object technique = ((Map<String, Object>) config.get(NORMALIZATION_CLAUSE)).get(TECHNIQUE_KEY);
        return Objects.isNull(technique) ? defaultTechnique : technique.toString().toLowerCase(Locale.ROOT);
    }

    @SuppressWarnings("unchecked")
    private static float[] readWeights(Map<String, Object> combinationClause) {
        if ((combinationClause.get(PARAMETERS_KEY) instanceof Map) == false) {
            return new float[0];
        }
        Map<String, Object> parameters = (Map<String, Object>) combinationClause.get(PARAMETERS_KEY);
        if ((parameters.get(WEIGHTS_KEY) instanceof List) == false) {
            return new float[0];
        }
        List<Object> raw = (List<Object>) parameters.get(WEIGHTS_KEY);
        float[] weights = new float[raw.size()];
        for (int i = 0; i < raw.size(); i++) {
            weights[i] = ((Number) raw.get(i)).floatValue();
        }
        return weights;
    }
}
