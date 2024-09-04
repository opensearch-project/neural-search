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

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Factory for query results RRF processor for search pipeline. Instantiates processor based on user provided input.
 * If user doesn't pass in value for rank constant, value defaults to 60.
 */
@AllArgsConstructor
@Log4j2
public class RRFProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    public static final String NORMALIZATION_CLAUSE = "combination";
    public static final String COMBINATION_CLAUSE = "combination";
    public static final String TECHNIQUE = "technique";
    public static final String PARAMETERS = "parameters";
    public static final int DEFAULT_RANK_CONSTANT = 60;

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
        Map<String, Object> normalizationClause = readOptionalMap(RRFProcessor.TYPE, tag, config, NORMALIZATION_CLAUSE);
        // reads parameters passed in from user to get rank constant to be used in RRFNormalizationTechnique
        Map<String, Object> normalizationParams = readOptionalMap(RRFProcessor.TYPE, tag, normalizationClause, PARAMETERS);
        int rankConstant = (int) normalizationParams.getOrDefault("rank_constant", DEFAULT_RANK_CONSTANT);
        ScoreNormalizationTechnique normalizationTechnique = ScoreNormalizationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(normalizationClause)) {
            String normalizationTechniqueName = readStringProperty(
                RRFProcessor.TYPE,
                tag,
                normalizationClause,
                TECHNIQUE,
                RRFNormalizationTechnique.TECHNIQUE_NAME
            );
            normalizationTechnique = scoreNormalizationFactory.createNormalization(normalizationTechniqueName);
        }

        Map<String, Object> combinationClause = readOptionalMap(RRFProcessor.TYPE, tag, config, COMBINATION_CLAUSE);

        ScoreCombinationTechnique scoreCombinationTechnique = ScoreCombinationFactory.RRF_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = readStringProperty(
                RRFProcessor.TYPE,
                tag,
                combinationClause,
                TECHNIQUE,
                RRFScoreCombinationTechnique.TECHNIQUE_NAME
            );
            // check for optional combination params
            Map<String, Object> combinationParams = readOptionalMap(RRFProcessor.TYPE, tag, combinationClause, PARAMETERS);
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique, combinationParams);
        }
        log.info(
            "Creating search phase results processor of type [{}] with normalization [{}] and combination [{}]",
            RRFProcessor.TYPE,
            normalizationTechnique,
            scoreCombinationTechnique
        );
        return new RRFProcessor(
            tag,
            description,
            normalizationTechnique,
            scoreCombinationTechnique,
            normalizationProcessorWorkflow,
            rankConstant
        );
    }
}
