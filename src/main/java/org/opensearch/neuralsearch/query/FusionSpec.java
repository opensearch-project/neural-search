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
 * <p>Two processor shapes are understood, mirroring the classic phase-results processors:
 * <ul>
 *   <li>{@code normalization-processor}: {@code normalization.technique} (min_max|l2|z_score) +
 *       {@code combination.technique} (arithmetic_mean) + optional {@code combination.parameters.weights}.</li>
 *   <li>{@code score-ranker-processor}: {@code combination.technique = rrf} + {@code combination.rank_constant} (on the
 *       combination clause itself, NOT under {@code parameters} — that is where {@code RRFProcessorFactory} reads it) +
 *       optional {@code combination.parameters.weights}. RRF is rank-based (no normalization clause).</li>
 * </ul>
 */
@Getter(AccessLevel.PACKAGE)
@Accessors(fluent = true)
public final class FusionSpec {

    // Combination techniques
    static final String TECHNIQUE_RRF = RRFScoreCombinationTechnique.TECHNIQUE_NAME;
    static final String TECHNIQUE_ARITHMETIC_MEAN = ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME;
    // Normalization techniques
    static final String NORMALIZATION_NONE = "none";
    static final String NORMALIZATION_MIN_MAX = MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME;

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

    private final String combinationTechnique; // rrf | arithmetic_mean
    private final String normalizationTechnique; // none | min_max | z_score | l2
    private final int rankConstant; // RRF only
    private final float[] weights; // per-leg weights; empty => unweighted

    FusionSpec(String combinationTechnique, String normalizationTechnique, int rankConstant, float[] weights) {
        this.combinationTechnique = combinationTechnique;
        this.normalizationTechnique = Objects.isNull(normalizationTechnique) ? NORMALIZATION_NONE : normalizationTechnique;
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
        return new FusionSpec(combination, normalization, DEFAULT_RANK_CONSTANT, weights);
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
        // RRF is rank based, so the score-ranker-processor has no normalization clause and this resolves to "none".
        // An inline fusion block can still carry one, and it is reported rather than dropped so the caller's technique
        // check rejects the contradictory pairing instead of silently ignoring what the user asked for.
        return new FusionSpec(TECHNIQUE_RRF, readNormalizationTechnique(config, NORMALIZATION_NONE), rankConstant, weights);
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
