/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.COMBINATION_CLAUSE;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.NORMALIZATION_CLAUSE;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.PARAMETERS;
import static org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory.TECHNIQUE;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;

public class NormalizationProcessorFactoryTests extends OpenSearchTestCase {

    private static final String NORMALIZATION_METHOD = "min_max";
    private static final String COMBINATION_METHOD = "arithmetic_mean";

    @SneakyThrows
    public void testNormalizationProcessor_whenNoParams_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
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
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }

    @SneakyThrows
    public void testNormalizationProcessor_whenTechniqueNamesNotSet_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put("normalization", new HashMap<>(Map.of()));
        config.put("combination", new HashMap<>(Map.of()));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }

    @SneakyThrows
    public void testNormalizationProcessor_whenWithParams_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put("normalization", new HashMap<>(Map.of("technique", "min_max")));
        config.put("combination", new HashMap<>(Map.of("technique", "arithmetic_mean")));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }

    @SneakyThrows
    public void testNormalizationProcessor_whenWithCombinationParams_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        double weight1 = RandomizedTest.randomDouble();
        double weight2 = 1.0f - weight1;
        Map<String, Object> config = new HashMap<>();
        config.put(NORMALIZATION_CLAUSE, new HashMap<>(Map.of("technique", "min_max")));
        config.put(
            COMBINATION_CLAUSE,
            new HashMap<>(
                Map.of(TECHNIQUE, "arithmetic_mean", PARAMETERS, new HashMap<>(Map.of("weights", Arrays.asList(weight1, weight2))))
            )
        );
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }

    @SneakyThrows
    public void testWeightsParams_whenInvalidValues_thenFail() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;

        // First value is always 0.5
        double first = 0.5;
        // Second value is random between 0.3 and 1.0
        double second = 0.3 + (RandomizedTest.randomDouble() * 0.7);
        // Third value is random between 0.3 and 1.0
        double third = 0.3 + (RandomizedTest.randomDouble() * 0.7);
        // This ensures minimum sum of 1.1 (0.5 + 0.3 + 0.3)

        Map<String, Object> config = new HashMap<>();
        config.put(NORMALIZATION_CLAUSE, new HashMap<>(Map.of("technique", "min_max")));
        config.put(
            COMBINATION_CLAUSE,
            new HashMap<>(
                Map.of(TECHNIQUE, "arithmetic_mean", PARAMETERS, new HashMap<>(Map.of("weights", Arrays.asList(first, second, third))))
            )
        );
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(processorFactories, tag, description, ignoreFailure, config, pipelineContext)
        );
        assertTrue(exception.getMessage().contains("sum of weights for combination must be equal to 1.0"));
    }

    public void testInputValidation_whenInvalidNormalizationClause_thenFail() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);

        expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, "")),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME))
                    )
                ),
                pipelineContext
            )
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, "random_name_for_normalization")),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME))
                    )
                ),
                pipelineContext
            )
        );
    }

    public void testInputValidation_whenInvalidCombinationClause_thenFail() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);

        expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, ""))
                    )
                ),
                pipelineContext
            )
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, "random_name_for_combination"))
                    )
                ),
                pipelineContext
            )
        );
    }

    public void testInputValidation_whenInvalidCombinationParams_thenFail() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            new ScoreNormalizationFactory(),
            new ScoreCombinationFactory()
        );
        Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);

        IllegalArgumentException exceptionBadTechnique = expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(
                            Map.of(
                                TECHNIQUE,
                                "",
                                NormalizationProcessorFactory.PARAMETERS,
                                new HashMap<>(Map.of("weights", "random_string"))
                            )
                        )
                    )
                ),
                pipelineContext
            )
        );
        assertThat(exceptionBadTechnique.getMessage(), containsString("provided combination technique is not supported"));

        IllegalArgumentException exceptionInvalidWeights = expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(
                            Map.of(
                                TECHNIQUE,
                                COMBINATION_METHOD,
                                NormalizationProcessorFactory.PARAMETERS,
                                new HashMap<>(Map.of("weights", 5.0))
                            )
                        )
                    )
                ),
                pipelineContext
            )
        );
        assertThat(exceptionInvalidWeights.getMessage(), containsString("parameter [weights] must be a collection of numbers"));

        IllegalArgumentException exceptionInvalidWeights2 = expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(
                            Map.of(
                                TECHNIQUE,
                                COMBINATION_METHOD,
                                NormalizationProcessorFactory.PARAMETERS,
                                new HashMap<>(Map.of("weights", new Boolean[] { true, false }))
                            )
                        )
                    )
                ),
                pipelineContext
            )
        );
        assertThat(exceptionInvalidWeights2.getMessage(), containsString("parameter [weights] must be a collection of numbers"));

        IllegalArgumentException exceptionInvalidParam = expectThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorFactory.create(
                processorFactories,
                tag,
                description,
                ignoreFailure,
                new HashMap<>(
                    Map.of(
                        NormalizationProcessorFactory.NORMALIZATION_CLAUSE,
                        new HashMap(Map.of(TECHNIQUE, NORMALIZATION_METHOD)),
                        NormalizationProcessorFactory.COMBINATION_CLAUSE,
                        new HashMap(
                            Map.of(
                                TECHNIQUE,
                                COMBINATION_METHOD,
                                NormalizationProcessorFactory.PARAMETERS,
                                new HashMap<>(Map.of("random_param", "value"))
                            )
                        )
                    )
                ),
                pipelineContext
            )
        );
        assertThat(
            exceptionInvalidParam.getMessage(),
            containsString("provided parameter for combination technique is not supported. supported parameters are [weights]")
        );
    }
}
