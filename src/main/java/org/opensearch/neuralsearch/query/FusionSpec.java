/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

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
 *       it — see {@link #readNormalizationTechnique}. An inline block may also spell {@code rank_constant} the other
 *       shape's way, under {@code normalization.parameters} — see {@link #resolveScoreRankerRankConstant}.</li>
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

    /**
     * The only {@code normalization.parameters} key fused mode honors, and only where {@code rrf} is the normalization
     * technique. Every other parameter classic reads from that clause — min_max's {@code lower_bounds} and
     * {@code upper_bounds} — has no coordinator-side implementation, so it is refused rather than accepted and dropped;
     * see {@link #rejectUnhonoredNormalizationParameters}.
     */
    private static final Set<String> RRF_NORMALIZATION_PARAMETERS = Set.of(RANK_CONSTANT_KEY);

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
        requireObjectClause(fusionConfig, NORMALIZATION_CLAUSE);
        requireObjectClause(fusionConfig, COMBINATION_CLAUSE);
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
        // Before anything reads a value out of `normalization.parameters`, so that whichever parameters this shape does
        // not honor are named in the error instead of being dropped on the floor.
        rejectUnhonoredNormalizationParameters(
            config,
            normalization,
            NORMALIZATION_RRF.equals(normalization) ? RRF_NORMALIZATION_PARAMETERS : Set.of()
        );
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
     * parameter error {@link #rejectUnhonoredNormalizationParameters} raises for it first.
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
        return readParameters((Map<String, Object>) config.get(NORMALIZATION_CLAUSE), NORMALIZATION_CLAUSE);
    }

    @SuppressWarnings("unchecked")
    private static FusionSpec fromScoreRankerProcessor(Map<String, Object> config) {
        String normalization = readNormalizationTechnique(config, NORMALIZATION_RRF);
        rejectUnhonoredNormalizationParameters(
            config,
            normalization,
            NORMALIZATION_RRF.equals(normalization) ? RRF_NORMALIZATION_PARAMETERS : Set.of()
        );
        Map<String, Object> combinationClause = Map.of();
        float[] weights = new float[0];
        if (config.get(COMBINATION_CLAUSE) instanceof Map) {
            combinationClause = (Map<String, Object>) config.get(COMBINATION_CLAUSE);
            rejectRankConstantUnderParameters(combinationClause);
            weights = readWeights(combinationClause);
        }
        int rankConstant = resolveScoreRankerRankConstant(combinationClause, readNormalizationParameters(config));
        // The score-ranker-processor has no normalization clause, so the default is what this shape almost always
        // resolves to. It is "rrf", not "none", because rank scoring IS this shape's normalization step — classic says so
        // itself: RRFProcessorFactory builds an RRFNormalizationTechnique for a processor with no normalization clause.
        // Naming it lets the coordinator resolve rrf through the same ScalarNormalizers lookup as every other technique.
        // An inline fusion block can still carry a normalization clause, and it is reported rather than dropped so the
        // caller's technique check rejects the contradictory pairing instead of silently ignoring what the user asked for.
        return new FusionSpec(Shape.SCORE_RANKER_PROCESSOR, TECHNIQUE_RRF, normalization, rankConstant, weights);
    }

    /**
     * {@code rank_constant} under {@code combination.parameters} is a config error, not a place we also look: the
     * score-ranker-processor rejects it there ("supported parameters are [weights]"). Fused mode rejects it too, rather
     * than silently falling back to the default 60 and mis-ranking every query for a user who put it in the wrong place.
     */
    private static void rejectRankConstantUnderParameters(Map<String, Object> combinationClause) {
        if (readParameters(combinationClause, COMBINATION_CLAUSE).containsKey(RANK_CONSTANT_KEY)) {
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

    /**
     * Refuse a {@code normalization.parameters} key fused mode does not honor, rather than parsing the config as if it
     * were not there. Every parameter classic reads from that clause changes the scores it produces, so accepting one and
     * ignoring it answers the request with a different ranking at HTTP 200 — min_max's {@code lower_bounds}/
     * {@code upper_bounds} being the case that matters, since they are the documented way to bound a score-based
     * normalization and there is no coordinator-side implementation of them yet: {@code ScalarNormalizers} resolves
     * min_max to a parameterless singleton, and the only parameter map the orchestrator ever builds carries
     * {@code rank_constant}. Refusing is what leaves that honoring for later without a behavior change; a config classic
     * accepts then fails fast in fused mode, which is a 400 the user can act on rather than silent divergence.
     *
     * <p>The honored set is the caller's, not this method's, because it is technique-dependent: {@code rank_constant} is
     * an rrf parameter, so it is honored wherever rrf is the normalization and refused under a score-based one — which is
     * also what keeps a stray {@code rank_constant} under {@code min_max} from being answered with a rank-constant range
     * error instead of an unsupported-parameter one.
     */
    private static void rejectUnhonoredNormalizationParameters(
        Map<String, Object> config,
        String normalization,
        Set<String> honoredParameters
    ) {
        Set<String> unhonored = new TreeSet<>(readNormalizationParameters(config).keySet());
        unhonored.removeAll(honoredParameters);
        if (unhonored.isEmpty()) {
            return;
        }
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "[%s.%s] %s not supported with normalization [%s] in fused mode; supported parameters are %s",
                NORMALIZATION_CLAUSE,
                PARAMETERS_KEY,
                unhonored,
                normalization,
                new TreeSet<>(honoredParameters)
            )
        );
    }

    /**
     * {@code rank_constant} for the {@code score-ranker-processor} shape, read from either place a user may reasonably
     * spell it: the combination clause itself, which is where {@code RRFProcessorFactory} reads it, or
     * {@code normalization.parameters}, which is where the {@code normalization-processor} shape keeps the very same
     * parameter. Both name one rank constant for one rrf normalization, so honoring either is unambiguous — and the
     * second spelling can only reach here from an inline {@code fusion} block, never from a pipeline: classic's
     * {@code RRFProcessorFactory} reads no normalization clause at all, so a {@code score-ranker-processor} carrying one
     * is rejected at pipeline creation. Honoring it therefore cannot make fused mode fuse a *pipeline* differently than
     * classic would.
     *
     * <p>Given in both places with different values it is a contradiction, not a precedence question, so it is refused
     * rather than resolved by a rule the user cannot see. Each location is resolved through the shared resolver, so a
     * non-integer or out-of-range value is reported exactly as classic reports it whichever place it was written.
     */
    private static int resolveScoreRankerRankConstant(Map<String, Object> combinationClause, Map<String, Object> normalizationParameters) {
        // Absent from the combination clause this is the default, which is also the right answer when neither place has it.
        int fromCombination = RRFScoreNormalizer.resolveRankConstant(combinationClause);
        if (normalizationParameters.containsKey(RANK_CONSTANT_KEY) == false) {
            return fromCombination;
        }
        int fromNormalization = RRFScoreNormalizer.resolveRankConstant(normalizationParameters);
        if (combinationClause.containsKey(RANK_CONSTANT_KEY) && fromCombination != fromNormalization) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] is set to [%d] on the [%s] clause and to [%d] under [%s.%s]; set it in one place only",
                    RANK_CONSTANT_KEY,
                    fromCombination,
                    COMBINATION_CLAUSE,
                    fromNormalization,
                    NORMALIZATION_CLAUSE,
                    PARAMETERS_KEY
                )
            );
        }
        return fromNormalization;
    }

    /**
     * Refuse a {@code normalization}/{@code combination} value that is not an object. Every reader below gates on
     * {@code instanceof Map}, so a bare string would fall through to the min_max + arithmetic_mean defaults and fuse by a
     * technique the user did not ask for, at HTTP 200. The mistake is an easy one to make, because one level up the
     * shorthand {@code "fusion": "pipeline"} <em>is</em> a bare string.
     */
    private static void requireObjectClause(Map<String, Object> fusionConfig, String clause) {
        Object value = fusionConfig.get(clause);
        if (Objects.isNull(value) || value instanceof Map) {
            return;
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] must be an object, got [%s]", clause, value));
    }

    /**
     * The {@code parameters} map of a {@code normalization} or {@code combination} clause, refusing a value that is present
     * but not an object instead of reading it as absent. This is {@link #requireObjectClause}'s guard one level down, and it
     * exists for the same reason: every reader of the map gated on {@code instanceof Map} and fell back to an empty one, so
     * a list or a bare string put {@link #rejectUnhonoredNormalizationParameters} in front of an empty key set — nothing to
     * report as unhonored — and the request fused with the technique's defaults at HTTP 200. That is exactly the
     * accepted-and-ignored scoring config the guard exists to make impossible, reachable by mistyping the container rather
     * than the parameter. An easy mistake to make, too, since the values inside are themselves lists:
     * {@code "parameters": [{"lower_bounds": [...]}]} is one stray bracket from the honest form.
     *
     * <p>Reported in this file's wording rather than the one core's {@code readOptionalMap} gives the same mistake in a
     * pipeline definition, because the two belong to different requests: a pipeline is refused at creation time, an inline
     * {@code fusion} block at query time, and only the second can reach here. Parity of wording is worth having where one
     * request could be answered by either path — {@link #weightsMustBeNumbers} — and not here.
     *
     * @param clause the {@code normalization} or {@code combination} clause to read {@code parameters} from
     * @param clauseName that clause's name, so the error names the full path the user wrote
     * @return the parameters map, empty when the key is absent or explicitly null
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> readParameters(Map<String, Object> clause, String clauseName) {
        Object parameters = clause.get(PARAMETERS_KEY);
        // Null is absent, matching requireObjectClause: an explicit "parameters": null asks for nothing, which is servable.
        if (Objects.isNull(parameters)) {
            return Map.of();
        }
        if (parameters instanceof Map) {
            return (Map<String, Object>) parameters;
        }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "[%s.%s] must be an object, got [%s]", clauseName, PARAMETERS_KEY, parameters)
        );
    }

    @SuppressWarnings("unchecked")
    private static String readNormalizationTechnique(Map<String, Object> config, String defaultTechnique) {
        if ((config.get(NORMALIZATION_CLAUSE) instanceof Map) == false) {
            return defaultTechnique;
        }
        Object technique = ((Map<String, Object>) config.get(NORMALIZATION_CLAUSE)).get(TECHNIQUE_KEY);
        return Objects.isNull(technique) ? defaultTechnique : technique.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * {@code combination.parameters.weights}, read the same way for both shapes — weights sit on the combination clause,
     * so the normalization technique has no bearing on them.
     *
     * <p>A {@code weights} that is present but not a list is refused rather than read as absent. The orchestrator hands
     * {@link org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil} a list it rebuilds from the parsed
     * array, so classic's own type check ("must be a collection of numbers") can never see the malformed value and a
     * scalar or string {@code weights} was fusing unweighted at HTTP 200 — the same request classic answers with a 400.
     *
     * <p>The elements are checked too, and that half is <em>stricter</em> than classic rather than parity with it: the list
     * check is the only one classic makes either ({@code ScoreCombinationUtil#validateParams}), and it then streams the
     * elements through {@code Double::floatValue}, so a list holding a non-numeric or null element fails classic and fused
     * mode alike with a cast failure at HTTP 500. Refused here with the reason instead — a request that cannot be served
     * should say what to fix.
     */
    @SuppressWarnings("unchecked")
    private static float[] readWeights(Map<String, Object> combinationClause) {
        Map<String, Object> parameters = readParameters(combinationClause, COMBINATION_CLAUSE);
        if ((parameters.get(WEIGHTS_KEY) instanceof List) == false) {
            if (parameters.containsKey(WEIGHTS_KEY)) {
                throw weightsMustBeNumbers();
            }
            return new float[0];
        }
        List<Object> raw = (List<Object>) parameters.get(WEIGHTS_KEY);
        float[] weights = new float[raw.size()];
        for (int i = 0; i < raw.size(); i++) {
            // Checked per element, because the cast is unchecked: a non-numeric or null element would otherwise fail the
            // request with a ClassCastException or a NullPointerException, at HTTP 500, saying nothing about what to fix.
            if ((raw.get(i) instanceof Number) == false) {
                throw weightsMustBeNumbers();
            }
            weights[i] = ((Number) raw.get(i)).floatValue();
        }
        return weights;
    }

    /** Classic's own wording for a malformed {@code weights}, so one request reads the same however it was refused. */
    private static IllegalArgumentException weightsMustBeNumbers() {
        return new IllegalArgumentException(String.format(Locale.ROOT, "parameter [%s] must be a collection of numbers", WEIGHTS_KEY));
    }
}
