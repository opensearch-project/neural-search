/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.Map;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.COMBINATION_CLAUSE;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.SUB_QUERY_SCORES;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.DEFAULT_SUB_QUERY_SCORES;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.TECHNIQUE;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.PARAMETERS;

/**
 * Factory class to instantiate RRF processor based on user provided input.
 */
@AllArgsConstructor
@Log4j2
public class RRFProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {

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
        // assign defaults
        ScoreNormalizationTechnique normalizationTechnique = scoreNormalizationFactory.createNormalization(
            RRFNormalizationTechnique.TECHNIQUE_NAME
        );
        ScoreCombinationTechnique scoreCombinationTechnique = scoreCombinationFactory.createCombination(
            RRFScoreCombinationTechnique.TECHNIQUE_NAME
        );

        Map<String, Object> combinationClause = readOptionalMap(RRFProcessor.TYPE, tag, config, COMBINATION_CLAUSE);
        boolean subQueryScores = readBooleanProperty(RRFProcessor.TYPE, tag, config, SUB_QUERY_SCORES, DEFAULT_SUB_QUERY_SCORES);
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = readStringProperty(
                RRFProcessor.TYPE,
                tag,
                combinationClause,
                TECHNIQUE,
                RRFScoreCombinationTechnique.TECHNIQUE_NAME
            );

            String rankConstantParam = RRFNormalizationTechnique.PARAM_NAME_RANK_CONSTANT;
            if (combinationClause.containsKey(rankConstantParam)) {
                normalizationTechnique = scoreNormalizationFactory.createNormalization(
                    RRFNormalizationTechnique.TECHNIQUE_NAME,
                    Map.of(rankConstantParam, combinationClause.get(rankConstantParam))
                );
            }
            Map<String, Object> params = readOptionalMap(RRFProcessor.TYPE, tag, combinationClause, PARAMETERS);
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique, params);
        }
        log.info(
            "Creating search phase results processor of type [{}] with normalization [{}] and combination [{}] with sub query scores as [{}]",
            RRFProcessor.TYPE,
            normalizationTechnique,
            scoreCombinationTechnique,
            subQueryScores
        );
        return new RRFProcessor(
            tag,
            description,
            normalizationTechnique,
            scoreCombinationTechnique,
            normalizationProcessorWorkflow,
            subQueryScores
        );
    }
}
