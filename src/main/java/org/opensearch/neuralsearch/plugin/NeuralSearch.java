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

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.analyzer.BertAnalyzerProvider;
import org.opensearch.neuralsearch.analyzer.BertTokenizerFactory;
import org.opensearch.neuralsearch.analyzer.TermWeightAnalyzerProvider;
import org.opensearch.neuralsearch.analyzer.TermWeightTokenizerFactory;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
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
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.index.analysis.AnalyzerComponents;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;

/**
 * Neural Search plugin class
 */
public class NeuralSearch extends Plugin implements ActionPlugin, SearchPlugin, IngestPlugin, ExtensiblePlugin, AnalysisPlugin {

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
