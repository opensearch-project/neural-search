/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class NeuralSearchTests extends OpenSearchQueryTestCase {

    public void testQuerySpecs() {
        NeuralSearch plugin = new NeuralSearch();
        List<SearchPlugin.QuerySpec<?>> querySpecs = plugin.getQueries();

        assertNotNull(querySpecs);
        assertFalse(querySpecs.isEmpty());
        assertTrue(querySpecs.stream().anyMatch(spec -> NeuralQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
        assertTrue(querySpecs.stream().anyMatch(spec -> HybridQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
    }

    public void testQueryPhaseSearcher() {
        NeuralSearch plugin = new NeuralSearch();
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
        NeuralSearch plugin = new NeuralSearch();
        Processor.Parameters processorParams = new Processor.Parameters(
            null,
            null,
            null,
            null,
            null,
            null,
            mock(IngestService.class),
            null,
            null
        );
        Map<String, Processor.Factory> processors = plugin.getProcessors(processorParams);
        assertNotNull(processors);
        assertNotNull(processors.get(TextEmbeddingProcessor.TYPE));
    }

    public void testSearchPhaseResultsProcessors() {
        NeuralSearch plugin = new NeuralSearch();
        SearchPipelinePlugin.Parameters parameters = mock(SearchPipelinePlugin.Parameters.class);
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

    public void testRequestProcessors() {
        NeuralSearch plugin = new NeuralSearch();
        SearchPipelinePlugin.Parameters parameters = mock(SearchPipelinePlugin.Parameters.class);
        Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchRequestProcessor>> processors = plugin.getRequestProcessors(
            parameters
        );
        assertNotNull(processors);
        assertNotNull(processors.get(NeuralQueryEnricherProcessor.TYPE));
    }

    public void testCreateComponentsInitialization() {
            
        NeuralSearch plugin = new NeuralSearch();
        ClusterService mockClusterService = mock(ClusterService.class);
        MLCommonsClientAccessor mockClientAccessor = mock(MLCommonsClientAccessor.class);
        
        // Mocking dependencies
        Client mockClient = mock(Client.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        ResourceWatcherService mockResourceWatcherService = mock(ResourceWatcherService.class);
        ScriptService mockScriptService = mock(ScriptService.class);
        NamedXContentRegistry mockXContentRegistry = mock(NamedXContentRegistry.class);
        Environment mockEnvironment = mock(Environment.class);
        NodeEnvironment mockNodeEnvironment = mock(NodeEnvironment.class);
        NamedWriteableRegistry mockNamedWriteableRegistry = mock(NamedWriteableRegistry.class);
        IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        Supplier<RepositoriesService> mockRepositoriesServiceSupplier = mock(Supplier.class);

        Collection<Object> components = plugin.createComponents(
            mockClient, mockClusterService, mockThreadPool, mockResourceWatcherService,
            mockScriptService, mockXContentRegistry, mockEnvironment, mockNodeEnvironment,
            mockNamedWriteableRegistry, mockIndexNameExpressionResolver, mockRepositoriesServiceSupplier
        );

        // Verify that the components are initialized
        assertNotNull(components);
        assertTrue(components.contains(mockClientAccessor));
       
}

}
