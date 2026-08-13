/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.Getter;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Processor for implementing reciprocal rank fusion technique on post
 * query search results. Updates query results with
 * normalized and combined scores for next phase (typically it's FETCH)
 * by using ranks from individual subqueries to calculate 'normalized'
 * scores before combining results from subqueries into final results
 */
@Log4j2
@AllArgsConstructor
public class RRFProcessor extends AbstractScoreHybridizationProcessor {
    public static final String TYPE = "score-ranker-processor";

    @Getter
    private final String tag;
    @Getter
    private final String description;
    @Getter(AccessLevel.PROTECTED)
    private final ScoreNormalizationTechnique normalizationTechnique;
    @Getter(AccessLevel.PROTECTED)
    private final ScoreCombinationTechnique combinationTechnique;
    @Getter(AccessLevel.PROTECTED)
    private final NormalizationProcessorWorkflow normalizationWorkflow;

    private final Map<String, Runnable> combTechniqueIncrementers = Map.of(
        RRFScoreCombinationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.COMB_TECHNIQUE_RRF_EXECUTIONS)
    );

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected void recordStats() {
        EventStatsManager.increment(EventStatName.RRF_PROCESSOR_EXECUTIONS);
        // ofNullable, not of: a technique with no incrementer must not fail the query. RRFProcessorFactory rejects any
        // technique but rrf, so this is no longer reachable from a pipeline definition, but this constructor is public.
        Optional.ofNullable(combTechniqueIncrementers.get(combinationTechnique.techniqueName())).ifPresent(Runnable::run);
    }
}
