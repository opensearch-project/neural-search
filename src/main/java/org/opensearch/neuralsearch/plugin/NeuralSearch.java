/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.plugin.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.transport.MLPredictAction;
import org.opensearch.neuralsearch.transport.MLPredictTransportAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Neural Search plugin class
 */
public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin {

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
        final MachineLearningNodeClient machineLearningNodeClient = new MachineLearningNodeClient(client);
        final MLCommonsClientAccessor clientAccessor = new MLCommonsClientAccessor(machineLearningNodeClient);
        NeuralQueryBuilder.initialize(clientAccessor);
        return List.of(clientAccessor);
    }

    /**
     * Registering the Action Handlers
     *
     * @return A {@link List} of {@link ActionHandler}
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(MLPredictAction.INSTANCE, MLPredictTransportAction.class));
    }

    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        final MachineLearningNodeClient machineLearningNodeClient = new MachineLearningNodeClient(parameters.client);
        final MLCommonsClientAccessor clientAccessor = new MLCommonsClientAccessor(machineLearningNodeClient);
        return Collections.singletonMap(TextEmbeddingProcessor.TYPE, new TextEmbeddingProcessorFactory(clientAccessor));
    }
}
