/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.ext.QuestionExtBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.AppendQueryResponseProcessor;
import org.opensearch.neuralsearch.processor.GenerativeTextLLMProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.GenerativeTextLLMProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.node.Node;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin, SearchPipelinePlugin {
    private MLCommonsClientAccessor clientAccessor;

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        NeuralQueryBuilder.initialize(getClientAccessor(client));
        return List.of(clientAccessor);
    }

    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(
            TextEmbeddingProcessor.TYPE,
            new TextEmbeddingProcessorFactory(getClientAccessor(parameters.client), parameters.env)
        );
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory> getProcessors(
        org.opensearch.search.pipeline.Processor.Parameters parameters
    ) {
        final Map<String, org.opensearch.search.pipeline.Processor.Factory> processorsMap = new HashMap<>();
        processorsMap.put(GenerativeTextLLMProcessor.TYPE, new GenerativeTextLLMProcessorFactory(getClientAccessor(parameters.client)));
        processorsMap.put(AppendQueryResponseProcessor.TYPE, new AppendQueryResponseProcessor.Factory());
        return processorsMap;
    }

    /**
     * This function ensures that 1 single MLClientAccessor is getting created. The reason why we cannot use
     * createComponents function to create {@link MLCommonsClientAccessor} is because createComponents gets called
     * after processors are registered. Check {@link Node} class for more details. We also cannot call this function
     * from Plugin constructor as constructor doesn't have the reference for {@link Client}.
     *
     * @param client {@link Client}
     * @return {@link MLCommonsClientAccessor}
     */
    private MLCommonsClientAccessor getClientAccessor(final Client client) {
        if (clientAccessor == null) {
            clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(client));
        }
        return clientAccessor;
    }

    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        return Collections.singletonList(new SearchExtSpec<>(QuestionExtBuilder.NAME, QuestionExtBuilder::new, QuestionExtBuilder::parse));
    }

}
