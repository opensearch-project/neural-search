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
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
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
public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin {

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
        NeuralQueryBuilder.initialize(clientAccessor);
        return List.of(clientAccessor);
    }

    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        return Collections.singletonMap(TextEmbeddingProcessor.TYPE, new TextEmbeddingProcessorFactory(clientAccessor, parameters.env));
    }
}
