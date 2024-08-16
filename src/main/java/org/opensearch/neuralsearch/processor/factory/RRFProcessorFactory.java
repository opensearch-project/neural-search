/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

import java.util.Map;
import java.util.Objects;

import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Factory for query results RRF processor for search pipeline. Instantiates processor based on user provided input.
 */
@AllArgsConstructor
@Log4j2
public class RRFProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    public static final String NORMALIZATION_CLAUSE = "normalization";
    public static final String COMBINATION_CLAUSE = "combination";
    public static final String TECHNIQUE = "technique";
    public static final String PARAMETERS = "parameters";
    public static final String RANK_CONSTANT = "rank_constant";

    private final NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private ScoreNormalizationFactory scoreNormalizationFactory;
    private ScoreCombinationFactory scoreCombinationFactory;
    public static int rankConstant = 0;
    public static final int x = 1;
    public static final int y = 0;



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
        ScoreNormalizationTechnique normalizationTechnique = ScoreNormalizationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(normalizationClause)) {
            String normalizationTechniqueName = readStringProperty(
                RRFProcessor.TYPE,
                tag,
                //normalizationClause,
                TECHNIQUE
                //MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME
            );
            //normalizationTechnique = scoreNormalizationFactory.createNormalization(normalizationTechniqueName);
        }

        Map<String, Object> combinationClause = readOptionalMap(RRFProcessor.TYPE, tag, config, COMBINATION_CLAUSE);

        ScoreCombinationTechnique scoreCombinationTechnique = ScoreCombinationFactory.DEFAULT_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = readStringProperty(
                RRFProcessor.TYPE,
                tag,
                combinationClause,
                TECHNIQUE,
                ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME
            );
            // check for optional combination params
            Map<String, Object> combinationParams = readOptionalMap(RRFProcessor.TYPE, tag, combinationClause, PARAMETERS);
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique, combinationParams);
        }
        log.info(
            "Creating search phase results processor of type [{}] with normalization [{}] and combination [{}]",
            RRFProcessor.TYPE//,
            //normalizationTechnique,
            //scoreCombinationTechnique
        );
        return new RRFProcessor(tag, description, rankConstant, x, y);//, normalizationTechnique, scoreCombinationTechnique, normalizationProcessorWorkflow);
    }
}
