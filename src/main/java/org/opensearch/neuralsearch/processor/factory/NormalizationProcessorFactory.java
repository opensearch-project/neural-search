/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;

import java.util.Map;
import java.util.Objects;

import lombok.AllArgsConstructor;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

/**
 * Factory for query results normalization processor for search pipeline. Instantiates processor based on user provided input.
 */
@AllArgsConstructor
public class NormalizationProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {
    private final NormalizationProcessorWorkflow normalizationProcessorWorkflow;
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
        String normalizationTechnique = Objects.isNull(normalizationClause)
            ? ScoreNormalizationTechnique.DEFAULT.name()
            : (String) normalizationClause.getOrDefault(NormalizationProcessor.TECHNIQUE, "");

        Map<String, Object> combinationClause = readOptionalMap(
            NormalizationProcessor.TYPE,
            tag,
            config,
            NormalizationProcessor.COMBINATION_CLAUSE
        );

        ScoreCombinationTechnique scoreCombinationTechnique = scoreCombinationFactory.DEFAULT_COMBINATION_METHOD;
        if (Objects.nonNull(combinationClause)) {
            String combinationTechnique = (String) combinationClause.getOrDefault(NormalizationProcessor.TECHNIQUE, "");
            scoreCombinationTechnique = scoreCombinationFactory.createCombination(combinationTechnique);
        }

        validateParameters(normalizationTechnique, tag);

        return new NormalizationProcessor(
            tag,
            description,
            ScoreNormalizationTechnique.valueOf(normalizationTechnique),
            scoreCombinationTechnique,
            normalizationProcessorWorkflow
        );
    }

    protected void validateParameters(final String normalizationTechniqueName, final String tag) {
        if (StringUtils.isEmpty(normalizationTechniqueName)) {
            throw newConfigurationException(
                NormalizationProcessor.TYPE,
                tag,
                NormalizationProcessor.TECHNIQUE,
                "normalization technique cannot be empty"
            );
        }
        if (!EnumUtils.isValidEnum(ScoreNormalizationTechnique.class, normalizationTechniqueName)) {
            throw newConfigurationException(
                NormalizationProcessor.TYPE,
                tag,
                NormalizationProcessor.TECHNIQUE,
                "provided normalization technique is not supported"
            );
        }
    }
}
