/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.NORMALIZATION_CLAUSE;
import static org.opensearch.neuralsearch.processor.factory.RRFProcessorFactory.PARAMETERS;
import static org.opensearch.neuralsearch.processor.factory.RRFProcessorFactory.COMBINATION_CLAUSE;
import static org.opensearch.neuralsearch.processor.factory.RRFProcessorFactory.TECHNIQUE;

public class RRFProcessorFactoryTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testDefaults_whenNoValuesPassed_thenSuccessful() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = rrfProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertRRFProcessor(searchPhaseResultsProcessor);
    }

    @SneakyThrows
    public void testCombinationParams_whenValidValues_thenSuccessful() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put(COMBINATION_CLAUSE, new HashMap<>(Map.of(TECHNIQUE, "rrf", PARAMETERS, new HashMap<>(Map.of("rank_constant", 100)))));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = rrfProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertRRFProcessor(searchPhaseResultsProcessor);
    }

    @SneakyThrows
    public void testInvalidCombinationParams_whenRankIsNegative_thenFail() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        Map<String, Object> config = new HashMap<>();
        config.put(COMBINATION_CLAUSE, new HashMap<>(Map.of(TECHNIQUE, "rrf", PARAMETERS, new HashMap<>(Map.of("rank_constant", -1)))));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> rrfProcessorFactory.create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
        );
        assertTrue(
            exception.getMessage().contains("rank constant must be in the interval between 1 and 10000, submitted rank constant: -1")
        );
    }

    @SneakyThrows
    public void testInvalidCombinationParams_whenRankIsTooLarge_thenFail() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        Map<String, Object> config = new HashMap<>();
        config.put(COMBINATION_CLAUSE, new HashMap<>(Map.of(TECHNIQUE, "rrf", PARAMETERS, new HashMap<>(Map.of("rank_constant", 50_000)))));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> rrfProcessorFactory.create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
        );
        assertTrue(
            exception.getMessage().contains("rank constant must be in the interval between 1 and 10000, submitted rank constant: 50000")
        );
    }

    @SneakyThrows
    public void testInvalidCombinationParams_whenRankIsNotNumeric_thenFail() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        Map<String, Object> config = new HashMap<>();
        config.put(
            COMBINATION_CLAUSE,
            new HashMap<>(Map.of(TECHNIQUE, "rrf", PARAMETERS, new HashMap<>(Map.of("rank_constant", "string"))))
        );
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> rrfProcessorFactory.create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
        );
        assertTrue(exception.getMessage().contains("parameter [rank_constant] must be an integer"));
    }

    @SneakyThrows
    public void testInvalidCombinationName_whenUnsupportedFunction_thenFail() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        Map<String, Object> config = new HashMap<>();
        config.put(
            COMBINATION_CLAUSE,
            new HashMap<>(Map.of(TECHNIQUE, "my_function", PARAMETERS, new HashMap<>(Map.of("rank_constant", 100))))
        );
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> rrfProcessorFactory.create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
        );
        assertTrue(exception.getMessage().contains("provided combination technique is not supported"));
    }

    @SneakyThrows
    public void testInvalidTechniqueType_whenPassingNormalization_thenSuccessful() {
        RRFProcessorFactory rrfProcessorFactory = new RRFProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        Map<String, Object> config = new HashMap<>();
        config.put(COMBINATION_CLAUSE, new HashMap<>(Map.of(TECHNIQUE, "rrf", PARAMETERS, new HashMap<>(Map.of("rank_constant", 100)))));
        config.put(
            NORMALIZATION_CLAUSE,
            new HashMap<>(Map.of(TECHNIQUE, ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME, PARAMETERS, new HashMap<>(Map.of())))
        );
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = rrfProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertRRFProcessor(searchPhaseResultsProcessor);
    }

    private static void assertRRFProcessor(SearchPhaseResultsProcessor searchPhaseResultsProcessor) {
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof RRFProcessor);
        RRFProcessor rrfProcessor = (RRFProcessor) searchPhaseResultsProcessor;
        assertEquals("score-ranker-processor", rrfProcessor.getType());
    }
}
