/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.plugin;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.RERANKER_MAX_DOC_FIELDS;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_STATS_ENABLED;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.highlight.SemanticHighlighter;
import org.opensearch.neuralsearch.highlight.SemanticHighlighterEngine;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;
import com.google.common.collect.ImmutableList;
import org.opensearch.action.ActionRequest;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.stats.info.InfoStatsManager;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MappingTransformer;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer;
import org.opensearch.neuralsearch.processor.factory.SemanticFieldProcessorFactory;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.executors.HybridQueryExecutor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NeuralSparseTwoPhaseProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.ExplanationResponseProcessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.factory.ExplanationResponseProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextChunkingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextImageEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.RRFProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsTransportAction;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import lombok.extern.log4j.Log4j2;

/**
 * Neural Search plugin class
 */
@Log4j2
public class NeuralSearch extends Plugin
    implements
        ActionPlugin,
        MapperPlugin,
        SearchPlugin,
        IngestPlugin,
        ExtensiblePlugin,
        SearchPipelinePlugin {
    private MLCommonsClientAccessor clientAccessor;
    private NamedXContentRegistry xContentRegistry;
    private NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private NeuralSearchSettingsAccessor settingsAccessor;
    private PipelineServiceUtil pipelineServiceUtil;
    private InfoStatsManager infoStatsManager;
    private final ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
    private final ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
    private final SemanticHighlighter semanticHighlighter;
    public static final String EXPLANATION_RESPONSE_KEY = "explanation_response";
    public static final String NEURAL_BASE_URI = "/_plugins/_neural";

    public NeuralSearch() {
        this.semanticHighlighter = new SemanticHighlighter();
    }

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
        NeuralSearchClusterUtil.instance().initialize(clusterService);
        NeuralQueryBuilder.initialize(clientAccessor);
        NeuralSparseQueryBuilder.initialize(clientAccessor);
        QueryTextExtractorRegistry queryTextExtractorRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine semanticHighlighterEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(clientAccessor)
            .queryTextExtractorRegistry(queryTextExtractorRegistry)
            .build();
        semanticHighlighter.initialize(semanticHighlighterEngine);
        HybridQueryExecutor.initialize(threadPool);
        normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());
        settingsAccessor = new NeuralSearchSettingsAccessor(clusterService, environment.settings());
        pipelineServiceUtil = new PipelineServiceUtil(clusterService);
        infoStatsManager = new InfoStatsManager(NeuralSearchClusterUtil.instance(), settingsAccessor, pipelineServiceUtil);
        EventStatsManager.instance().initialize(settingsAccessor);
        this.xContentRegistry = xContentRegistry;
        return List.of(clientAccessor, EventStatsManager.instance(), infoStatsManager);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent),
            new QuerySpec<>(HybridQueryBuilder.NAME, HybridQueryBuilder::new, HybridQueryBuilder::fromXContent),
            new QuerySpec<>(NeuralSparseQueryBuilder.NAME, NeuralSparseQueryBuilder::new, NeuralSparseQueryBuilder::fromXContent),
            new QuerySpec<>(NeuralKNNQueryBuilder.NAME, NeuralKNNQueryBuilder::new, NeuralKNNQueryBuilder::fromXContent)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor);
        return ImmutableList.of(restNeuralStatsAction);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(NeuralStatsAction.INSTANCE, NeuralStatsTransportAction.class));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(HybridQueryExecutor.getExecutorBuilder(settings));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        return Map.of(
            TextEmbeddingProcessor.TYPE,
            new TextEmbeddingProcessorFactory(
                parameters.client,
                clientAccessor,
                parameters.env,
                parameters.ingestService.getClusterService()
            ),
            SparseEncodingProcessor.TYPE,
            new SparseEncodingProcessorFactory(
                parameters.client,
                clientAccessor,
                parameters.env,
                parameters.ingestService.getClusterService()
            ),
            TextImageEmbeddingProcessor.TYPE,
            new TextImageEmbeddingProcessorFactory(
                parameters.client,
                clientAccessor,
                parameters.env,
                parameters.ingestService.getClusterService()
            ),
            TextChunkingProcessor.TYPE,
            new TextChunkingProcessorFactory(parameters.env, parameters.ingestService.getClusterService(), parameters.analysisRegistry)
        );
    }

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        return Optional.of(new HybridQueryPhaseSearcher());
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(
        Parameters parameters
    ) {
        return Map.of(
            NormalizationProcessor.TYPE,
            new NormalizationProcessorFactory(normalizationProcessorWorkflow, scoreNormalizationFactory, scoreCombinationFactory),
            RRFProcessor.TYPE,
            new RRFProcessorFactory(normalizationProcessorWorkflow, scoreNormalizationFactory, scoreCombinationFactory)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(RERANKER_MAX_DOC_FIELDS, NEURAL_STATS_ENABLED);
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchRequestProcessor>> getRequestProcessors(
        Parameters parameters
    ) {
        return Map.of(
            NeuralQueryEnricherProcessor.TYPE,
            new NeuralQueryEnricherProcessor.Factory(),
            NeuralSparseTwoPhaseProcessor.TYPE,
            new NeuralSparseTwoPhaseProcessor.Factory()
        );
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchResponseProcessor>> getResponseProcessors(
        Parameters parameters
    ) {
        return Map.of(
            RerankProcessor.TYPE,
            new RerankProcessorFactory(clientAccessor, parameters.searchPipelineService.getClusterService()),
            ExplanationResponseProcessor.TYPE,
            new ExplanationResponseProcessorFactory()
        );
    }

    @Override
    public List<SearchPlugin.SearchExtSpec<?>> getSearchExts() {
        return List.of(
            new SearchExtSpec<>(
                RerankSearchExtBuilder.PARAM_FIELD_NAME,
                in -> new RerankSearchExtBuilder(in),
                parser -> RerankSearchExtBuilder.parse(parser)
            )
        );
    }

    /**
     * Register semantic highlighter
     */
    @Override
    public Map<String, Highlighter> getHighlighters() {
        return Collections.singletonMap(SemanticHighlighter.NAME, semanticHighlighter);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(SemanticFieldMapper.CONTENT_TYPE, new SemanticFieldMapper.TypeParser());
    }

    @Override
    public List<MappingTransformer> getMappingTransformers() {
        return List.of(new SemanticMappingTransformer(clientAccessor, xContentRegistry));
    }

    @Override
    public Map<String, Processor.Factory> getSystemIngestProcessors(Processor.Parameters parameters) {
        return Map.of(
            SemanticFieldProcessorFactory.PROCESSOR_FACTORY_TYPE,
            new SemanticFieldProcessorFactory(
                clientAccessor,
                parameters.env,
                parameters.ingestService.getClusterService(),
                parameters.analysisRegistry
            )
        );
    }
}
