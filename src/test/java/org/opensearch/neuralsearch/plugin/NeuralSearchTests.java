/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.plugin;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NeuralSparseTwoPhaseProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.plugins.SearchPlugin.SearchExtSpec;
import org.opensearch.search.pipeline.Processor.Factory;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;

public class NeuralSearchTests extends OpenSearchQueryTestCase {

    private NeuralSearch plugin;

    @Mock
    private SearchPipelineService searchPipelineService;
    private SearchPipelinePlugin.Parameters parameters;
    @Mock
    private ClusterService clusterService;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        plugin = new NeuralSearch();

        when(searchPipelineService.getClusterService()).thenReturn(clusterService);
        parameters = new SearchPipelinePlugin.Parameters(null, null, null, null, null, null, searchPipelineService, null, null, null);
    }

    public void testCreateComponents() {
        // Empty method to explain why createComponents isn't covered by tests
        //
        // CreateComponents uses initialize() methods on three other classes which sets static class variables.
        // This breaks the encapsulation of tests and produces unpredictable behavior in these other tests.
        // It's possible to work around these limitations but not worth it just to cover some lines of code
    }

    public void testQuerySpecs() {
        List<SearchPlugin.QuerySpec<?>> querySpecs = plugin.getQueries();

        assertNotNull(querySpecs);
        assertFalse(querySpecs.isEmpty());
        assertTrue(querySpecs.stream().anyMatch(spec -> NeuralQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
        assertTrue(querySpecs.stream().anyMatch(spec -> HybridQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
    }

    public void testQueryPhaseSearcher() {
        Optional<QueryPhaseSearcher> queryPhaseSearcherWithFeatureFlagDisabled = plugin.getQueryPhaseSearcher();

        assertNotNull(queryPhaseSearcherWithFeatureFlagDisabled);
        assertFalse(queryPhaseSearcherWithFeatureFlagDisabled.isEmpty());
        assertTrue(queryPhaseSearcherWithFeatureFlagDisabled.get() instanceof HybridQueryPhaseSearcher);

        initFeatureFlags();

        Optional<QueryPhaseSearcher> queryPhaseSearcher = plugin.getQueryPhaseSearcher();

        assertNotNull(queryPhaseSearcher);
        assertTrue(queryPhaseSearcher.isEmpty());
    }

    public void testProcessors() {
        Settings settings = Settings.builder().build();
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(settings);
        Processor.Parameters processorParams = new Processor.Parameters(
            environment,
            null,
            null,
            null,
            null,
            null,
            mock(IngestService.class),
            null,
            null,
            mock(IndicesService.class)
        );
        Map<String, Processor.Factory> processors = plugin.getProcessors(processorParams);
        assertNotNull(processors);
        assertNotNull(processors.get(TextEmbeddingProcessor.TYPE));
    }

    public void testSearchPhaseResultsProcessors() {
        Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor>> searchPhaseResultsProcessors = plugin
            .getSearchPhaseResultsProcessors(parameters);
        assertNotNull(searchPhaseResultsProcessors);
        assertEquals(1, searchPhaseResultsProcessors.size());
        assertTrue(searchPhaseResultsProcessors.containsKey("normalization-processor"));
        org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor> scoringProcessor = searchPhaseResultsProcessors.get(
            NormalizationProcessor.TYPE
        );
        assertTrue(scoringProcessor instanceof NormalizationProcessorFactory);
    }

    public void testGetSettings() {
        List<Setting<?>> settings = plugin.getSettings();

        assertEquals(2, settings.size());
    }

    public void testRequestProcessors() {
        Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchRequestProcessor>> processors = plugin.getRequestProcessors(
            parameters
        );
        assertNotNull(processors);
        assertNotNull(processors.get(NeuralQueryEnricherProcessor.TYPE));
        assertNotNull(processors.get(NeuralSparseTwoPhaseProcessor.TYPE));
    }

    public void testResponseProcessors() {
        Map<String, Factory<SearchResponseProcessor>> processors = plugin.getResponseProcessors(parameters);
        assertNotNull(processors);
        assertNotNull(processors.get(RerankProcessor.TYPE));
    }

    public void testSearchExts() {
        List<SearchExtSpec<?>> searchExts = plugin.getSearchExts();

        assertEquals(1, searchExts.size());
    }

    public void testExecutionBuilders() {
        Settings settings = Settings.builder().build();
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(settings);
        final List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);

        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals("Unexpected number of executor builders are registered", 1, executorBuilders.size());
        assertTrue(executorBuilders.get(0) instanceof FixedExecutorBuilder);
    }
}
