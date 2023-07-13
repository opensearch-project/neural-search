/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportSettings;
import org.opensearch.watcher.ResourceWatcherService;

import com.google.common.annotations.VisibleForTesting;

/**
 * Neural Search plugin class
 */
public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin {
    /**
     * Gates the functionality of hybrid search
     * Currently query phase searcher added with hybrid search will conflict with concurrent search in core.
     * Once that problem is resolved this feature flag can be removed.
     * Key is the name string, value is key + transport feature specific prefix,
     * prefix is added by core when we register feature (https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/plugins/PluginsService.java#L277)
     * We need to write and read by the value, key is only for definition
     */
    @VisibleForTesting
    public static final Pair<String, String> NEURAL_SEARCH_HYBRID_SEARCH_DISABLED = Pair.of(
        "neural_search_hybrid_search_disabled",
        String.join(".", TransportSettings.FEATURE_PREFIX, "neural_search_hybrid_search_disabled")
    );
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

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent),
            new QuerySpec<>(HybridQueryBuilder.NAME, HybridQueryBuilder::new, HybridQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        return Collections.singletonMap(TextEmbeddingProcessor.TYPE, new TextEmbeddingProcessorFactory(clientAccessor, parameters.env));
    }

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        if (FeatureFlags.isEnabled(NEURAL_SEARCH_HYBRID_SEARCH_DISABLED.getValue())) {
            return Optional.empty();
        }
        return Optional.of(new HybridQueryPhaseSearcher());
    }

    @Override
    protected Optional<String> getFeature() {
        return Optional.of(NEURAL_SEARCH_HYBRID_SEARCH_DISABLED.getKey());
    }
}
