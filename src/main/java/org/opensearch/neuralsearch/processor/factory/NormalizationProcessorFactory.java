/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;

import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;

import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

/**
 * Factory for query results normalization processor for search pipeline. Instantiates processor based on user provided input.
 */
@AllArgsConstructor
public class NormalizationProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    private final NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private ScoreNormalizationFactory scoreNormalizationFactory;
    private ScoreCombinationFactory scoreCombinationFactory;

    @Override
    public SearchPhaseResultsProcessor create(
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) throws Exception {
        Map<String, Object> normalizationClause = readOptionalMap(
            NormalizationProcessor.TYPE,
            tag,
            config,
            NormalizationProcessor.NORMALIZATION_CLAUSE
        );
        ScoreNormalizationTechnique normalizationTechnique = ScoreNormalizationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(normalizationClause)) {
            String combinationTechnique = (String) normalizationClause.getOrDefault(NormalizationProcessor.TECHNIQUE, "");
            normalizationTechnique = scoreNormalizationFactory.createNormalization(combinationTechnique);
        }

        Map<String, Object> combinationClause = readOptionalMap(
            NormalizationProcessor.TYPE,
            tag,
            config,
            NormalizationProcessor.COMBINATION_CLAUSE
        );

        ScoreCombinationTechnique scoreCombinationTechnique = ScoreCombinationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = (String) combinationClause.getOrDefault(NormalizationProcessor.TECHNIQUE, "");
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique);
        }

        return new NormalizationProcessor(
            tag,
            description,
            normalizationTechnique,
            scoreCombinationTechnique,
            normalizationProcessorWorkflow
        );
    }
}
