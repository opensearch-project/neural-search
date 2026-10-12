/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.Optional;

import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.GeometricMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.HarmonicMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizationTechnique;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * Processor for score normalization and combination on post query search results. Updates query results with
 * normalized and combined scores for next phase (typically it's FETCH)
 */
@Log4j2
@AllArgsConstructor
public class NormalizationProcessor extends AbstractScoreHybridizationProcessor {
    public static final String TYPE = "normalization-processor";

    private final String tag;
    private final String description;
    @Getter(AccessLevel.PROTECTED)
    private final ScoreNormalizationTechnique normalizationTechnique;
    @Getter(AccessLevel.PROTECTED)
    private final ScoreCombinationTechnique combinationTechnique;
    @Getter(AccessLevel.PROTECTED)
    private final NormalizationProcessorWorkflow normalizationWorkflow;

    private final Map<String, Runnable> normTechniqueIncrementers = Map.of(
        L2ScoreNormalizationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.NORM_TECHNIQUE_L2_EXECUTIONS),
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.NORM_TECHNIQUE_MINMAX_EXECUTIONS),
        ZScoreNormalizationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.NORM_TECHNIQUE_NORM_ZSCORE_EXECUTIONS)
    );

    private final Map<String, Runnable> combTechniqueIncrementers = Map.of(
        ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.COMB_TECHNIQUE_ARITHMETIC_EXECUTIONS),
        HarmonicMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.COMB_TECHNIQUE_HARMONIC_EXECUTIONS),
        GeometricMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.COMB_TECHNIQUE_GEOMETRIC_EXECUTIONS)
    );

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    protected void recordStats() {
        EventStatsManager.increment(EventStatName.NORMALIZATION_PROCESSOR_EXECUTIONS);
        Optional.ofNullable(normTechniqueIncrementers.get(normalizationTechnique.techniqueName())).ifPresent(Runnable::run);
        Optional.ofNullable(combTechniqueIncrementers.get(combinationTechnique.techniqueName())).ifPresent(Runnable::run);
    }
}
