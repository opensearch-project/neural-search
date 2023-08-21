/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.analyzer.BertAnalyzerProvider;
import org.opensearch.neuralsearch.analyzer.BertTokenizerFactory;
import org.opensearch.neuralsearch.analyzer.TermWeightAnalyzerProvider;
import org.opensearch.neuralsearch.analyzer.TermWeightTokenizerFactory;
import org.opensearch.neuralsearch.ml.MLCommonsTextEmbeddingClientAccessor;
import org.opensearch.neuralsearch.ml.MLCommonsNeuralSparseClientAccessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.NeuralSparseDocumentProcessor;
import org.opensearch.neuralsearch.processor.factory.NeuralSparseProcessorFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import com.google.common.collect.ImmutableMap;

/**
 * Neural Search plugin class
 */
public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin, AnalysisPlugin {

    private MLCommonsTextEmbeddingClientAccessor clientTEAccessor;
    private MLCommonsNeuralSparseClientAccessor clientNSAccessor;

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
        NeuralQueryBuilder.initialize(clientTEAccessor);
        NeuralSparseQueryBuilder.initialize(clientNSAccessor);
        return List.of(clientTEAccessor, clientNSAccessor);
    }

    public List<QuerySpec<?>> getQueries() {
        var qs1 = new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent);
        var qs2 = new QuerySpec<>(
                NeuralSparseQueryBuilder.NAME,
                NeuralSparseQueryBuilder::new,
                NeuralSparseQueryBuilder::fromXContent
        );

        return List.of(qs1, qs2);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientTEAccessor = new MLCommonsTextEmbeddingClientAccessor(new MachineLearningNodeClient(parameters.client));
        clientNSAccessor = new MLCommonsNeuralSparseClientAccessor(new MachineLearningNodeClient(parameters.client));
        return ImmutableMap.of(TextEmbeddingProcessor.TYPE, new TextEmbeddingProcessorFactory(clientTEAccessor, parameters.env), NeuralSparseDocumentProcessor.TYPE, new NeuralSparseProcessorFactory(clientNSAccessor, parameters.env));
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> extra = new HashMap<>();

        extra.put("bert", BertTokenizerFactory::getBertTokenizerFactory);
        extra.put("term_weight", TermWeightTokenizerFactory::getTermWeightTokenizerFactory);

        return extra;
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> extra = new HashMap<>();

        extra.put("bert", BertAnalyzerProvider::getBertAnalyzerProvider);
        extra.put("term_weight", TermWeightAnalyzerProvider::geTermWeightAnalyzerProvider);

        return extra;
    }
}
