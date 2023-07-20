/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.ScoreNormalizationTechnique;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;

/**
 * Factory for query results normalization processor for search pipeline. Instantiates processor based on user provided input.
 */
public class NormalizationProcessorFactory implements Processor.Factory<SearchPhaseResultsProcessor> {

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
        String combinationTechnique = Objects.isNull(combinationClause)
            ? ScoreCombinationTechnique.DEFAULT.name()
            : (String) combinationClause.getOrDefault(NormalizationProcessor.TECHNIQUE, "");

        validateParameters(normalizationTechnique, combinationTechnique);

        return new NormalizationProcessor(
            tag,
            description,
            ScoreNormalizationTechnique.valueOf(normalizationTechnique),
            ScoreCombinationTechnique.valueOf(combinationTechnique)
        );
    }

    protected void validateParameters(final String normalizationTechniqueName, final String combinationTechniqueName) {
        if (StringUtils.isEmpty(normalizationTechniqueName)) {
            throw new IllegalArgumentException("normalization technique cannot be empty");
        }
        if (StringUtils.isEmpty(combinationTechniqueName)) {
            throw new IllegalArgumentException("combination technique cannot be empty");
        }
        if (!EnumUtils.isValidEnum(ScoreNormalizationTechnique.class, normalizationTechniqueName)) {
            throw new IllegalArgumentException("provided normalization technique is not supported");
        }
        if (!EnumUtils.isValidEnum(ScoreCombinationTechnique.class, combinationTechniqueName)) {
            throw new IllegalArgumentException("provided combination technique is not supported");
        }
    }
}
