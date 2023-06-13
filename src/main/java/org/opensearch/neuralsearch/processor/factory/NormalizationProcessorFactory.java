/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalMap;

import java.util.Map;

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
        String normalizationTechnique = normalizationClause == null
            ? ScoreNormalizationTechnique.DEFAULT.name()
            : (String) normalizationClause.get(NormalizationProcessor.TECHNIQUE);

        Map<String, Object> combinationClause = readOptionalMap(
            NormalizationProcessor.TYPE,
            tag,
            config,
            NormalizationProcessor.COMBINATION_CLAUSE
        );
        String combinationTechnique = combinationClause == null
            ? ScoreCombinationTechnique.DEFAULT.name()
            : (String) combinationClause.get(NormalizationProcessor.TECHNIQUE);

        return new NormalizationProcessor(tag, description, normalizationTechnique, combinationTechnique);
    }
}
