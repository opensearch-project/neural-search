/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.plugin;

import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.HIGHLIGHTER_TYPE;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.RERANKER_MAX_DOC_FIELDS;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_STATS_ENABLED;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.SEMANTIC_INGEST_BATCH_SIZE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.highlight.SemanticHighlighter;
import org.opensearch.neuralsearch.highlight.single.SemanticHighlighterEngine;
import org.opensearch.neuralsearch.highlight.single.extractor.QueryTextExtractorRegistry;
import com.google.common.collect.ImmutableList;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.ActionRequest;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.search.collector.HybridQueryCollectorContextSpecFactory;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.neuralsearch.rest.RestNeuralSparseClearCacheHandler;
import org.opensearch.neuralsearch.rest.RestNeuralSparseWarmupHandler;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.stats.info.InfoStatsManager;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MappingTransformer;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer;
import org.opensearch.neuralsearch.processor.factory.SemanticFieldProcessorFactory;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.transport.client.Client;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.processor.SemanticHighlightingFactory;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.rest.RestNeuralSparseClearCacheHandler;
import org.opensearch.neuralsearch.rest.RestNeuralSparseWarmupHandler;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.sparse.SparseIndexEventListener;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.cache.CircuitBreakerManager;
import org.opensearch.neuralsearch.sparse.cache.MemoryUsageManager;
import org.opensearch.neuralsearch.sparse.codec.SparseCodecService;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.stats.info.InfoStatsManager;
import org.opensearch.index.mapper.MappingTransformer;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer;
import org.opensearch.neuralsearch.processor.factory.SemanticFieldProcessorFactory;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.CircuitBreakerPlugin;
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

import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.executors.HybridQueryExecutor;
import org.opensearch.neuralsearch.highlight.SemanticHighlighter;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.AgenticQueryTranslatorProcessor;
import org.opensearch.neuralsearch.processor.AgenticContextResponseProcessor;
import org.opensearch.neuralsearch.processor.ExplanationResponseProcessor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NeuralSparseTwoPhaseProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.RRFProcessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.factory.ExplanationResponseProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.RRFProcessorFactory;
import org.opensearch.neuralsearch.highlight.batch.processor.SemanticHighlightingFactory;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.processor.factory.TextChunkingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextImageEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder;
import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.sparse.SparseIndexEventListener;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldMapper;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsTransportAction;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheAction;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheTransportAction;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupAction;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupTransportAction;

import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
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
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.DEFAULT_INDEX_THREAD_QTY;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_NAME;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_OVERHEAD;

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
        SearchPipelinePlugin,
        EnginePlugin,
        CircuitBreakerPlugin {
    private MLCommonsClientAccessor clientAccessor;
    private NamedXContentRegistry xContentRegistry;
    private NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private NeuralSearchSettingsAccessor settingsAccessor;
    private PipelineServiceUtil pipelineServiceUtil;
    private InfoStatsManager infoStatsManager;
    private ClusterService clusterService;
    private final SemanticHighlighter semanticHighlighter;
    private final ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
    private final ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();
    public static final String EXPLANATION_RESPONSE_KEY = "explanation_response";
    public static final String NEURAL_BASE_URI = "/_plugins/_neural";

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
        this.clusterService = clusterService;
        // Create clientAccessor first as it's needed by other components
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(client));

        NeuralSearchClusterUtil.instance().initialize(clusterService, indexNameExpressionResolver);
        NeuralQueryBuilder.initialize(clientAccessor);
        NeuralSparseQueryBuilder.initialize(clientAccessor);
        HybridQueryExecutor.initialize(threadPool);
        normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());
        settingsAccessor = new NeuralSearchSettingsAccessor(clusterService, environment.settings());
        pipelineServiceUtil = new PipelineServiceUtil(clusterService);
        infoStatsManager = new InfoStatsManager(NeuralSearchClusterUtil.instance(), settingsAccessor, pipelineServiceUtil);
        EventStatsManager.instance().initialize(settingsAccessor);
        this.xContentRegistry = xContentRegistry;
        ClusterTrainingExecutor.getInstance().initialize(threadPool);

        // Initialize SemanticHighlighterEngine for legacy non-batch highlighting
        QueryTextExtractorRegistry queryTextExtractorRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine semanticHighlighterEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(clientAccessor)
            .queryTextExtractorRegistry(queryTextExtractorRegistry)
            .build();

        // Initialize the semantic highlighter
        this.semanticHighlighter.initialize(semanticHighlighterEngine);
        this.semanticHighlighter.setClusterService(clusterService);

        return List.of(clientAccessor, EventStatsManager.instance(), infoStatsManager);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent),
            new QuerySpec<>(HybridQueryBuilder.NAME, HybridQueryBuilder::new, HybridQueryBuilder::fromXContent),
            new QuerySpec<>(NeuralSparseQueryBuilder.NAME, NeuralSparseQueryBuilder::new, NeuralSparseQueryBuilder::fromXContent),
            new QuerySpec<>(NeuralKNNQueryBuilder.NAME, NeuralKNNQueryBuilder::new, NeuralKNNQueryBuilder::fromXContent),
            new QuerySpec<>(AgenticSearchQueryBuilder.NAME, AgenticSearchQueryBuilder::new, AgenticSearchQueryBuilder::fromXContent)
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
        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, NeuralSearchClusterUtil.instance());
        RestNeuralSparseWarmupHandler restNeuralSparseWarmupCacheHandler = new RestNeuralSparseWarmupHandler(
            NeuralSearchClusterUtil.instance().getClusterService(),
            indexNameExpressionResolver
        );
        RestNeuralSparseClearCacheHandler restNeuralSparseClearCacheHandler = new RestNeuralSparseClearCacheHandler(
            NeuralSearchClusterUtil.instance().getClusterService(),
            indexNameExpressionResolver
        );
        return ImmutableList.of(restNeuralStatsAction, restNeuralSparseWarmupCacheHandler, restNeuralSparseClearCacheHandler);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(NeuralStatsAction.INSTANCE, NeuralStatsTransportAction.class),
            new ActionHandler<>(NeuralSparseWarmupAction.INSTANCE, NeuralSparseWarmupTransportAction.class),
            new ActionHandler<>(NeuralSparseClearCacheAction.INSTANCE, NeuralSparseClearCacheTransportAction.class)
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            HybridQueryExecutor.getExecutorBuilder(settings),
            new FixedExecutorBuilder(
                settings,
                SparseConstants.THREAD_POOL_NAME,
                DEFAULT_INDEX_THREAD_QTY,
                -1,
                SparseConstants.THREAD_POOL_NAME,
                false
            )
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        // clientAccessor is already initialized in createComponents
        if (clientAccessor == null) {
            // Fallback initialization if createComponents wasn't called (e.g., in some test scenarios)
            clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        }
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
        return List.of(
            RERANKER_MAX_DOC_FIELDS,
            NEURAL_STATS_ENABLED,
            SEMANTIC_INGEST_BATCH_SIZE,
            HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY,
            SparseSettings.IS_SPARSE_INDEX_SETTING,
            NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
            NEURAL_CIRCUIT_BREAKER_LIMIT,
            NEURAL_CIRCUIT_BREAKER_OVERHEAD
        );
    }

    @Override
    public Settings additionalSettings() {
        // System processor for batch semantic highlighting is not enabled by default
        // Users must explicitly enable it in opensearch.yml:
        // search.pipeline.enabled_system_generated_factories:
        // ["org.opensearch.neuralsearch.highlight.SemanticHighlightingProcessorFactory"]
        return Settings.EMPTY;
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchRequestProcessor>> getRequestProcessors(
        Parameters parameters
    ) {
        return Map.of(
            NeuralQueryEnricherProcessor.TYPE,
            new NeuralQueryEnricherProcessor.Factory(),
            NeuralSparseTwoPhaseProcessor.TYPE,
            new NeuralSparseTwoPhaseProcessor.Factory(),
            AgenticQueryTranslatorProcessor.TYPE,
            new AgenticQueryTranslatorProcessor.Factory(clientAccessor, xContentRegistry, settingsAccessor)
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
            new ExplanationResponseProcessorFactory(),
            SemanticHighlightingConstants.PROCESSOR_TYPE,
            new SemanticHighlightingResponseProcessorFactory(clientAccessor)
            new ExplanationResponseProcessorFactory(),
            AgenticContextResponseProcessor.TYPE,
            new AgenticContextResponseProcessor.Factory()
        );
    }

    @Override
    public Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> getSystemGeneratedResponseProcessors(
        Parameters parameters
    ) {
        // System-generated semantic highlighting processor that automatically applies when semantic highlighting is detected
        return Map.of(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE, new SemanticHighlightingFactory(clientAccessor));
    }

    @Override
    public List<SearchPlugin.SearchExtSpec<?>> getSearchExts() {
        return List.of(
            new SearchExtSpec<>(
                RerankSearchExtBuilder.PARAM_FIELD_NAME,
                in -> new RerankSearchExtBuilder(in),
                parser -> RerankSearchExtBuilder.parse(parser)
            ),
            new SearchExtSpec<>(AgentStepsSearchExtBuilder.AGENT_STEPS_FIELD_NAME, in -> new AgentStepsSearchExtBuilder(in), parser -> {
                throw new UnsupportedOperationException("AgentStepsSearchExtBuilder should not be parsed from request");
            })
        );
    }

    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(SparseSettings.IS_SPARSE_INDEX_SETTING)) {
            return Optional.of(SparseCodecService::new);
        }
        return Optional.empty();
    }

    /**
     * Register hybrid semantic highlighter that supports both batch and non-batch modes
     * - Batch mode: Requires system processor to be explicitly enabled
     * - Non-batch mode: Uses legacy highlighting for backward compatibility
     */
    @Override
    public Map<String, Highlighter> getHighlighters() {
        return Collections.singletonMap(HIGHLIGHTER_TYPE, semanticHighlighter);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
            SemanticFieldMapper.CONTENT_TYPE,
            new SemanticFieldMapper.TypeParser(),
            SparseVectorFieldMapper.CONTENT_TYPE,
            new SparseVectorFieldMapper.SparseTypeParser()
        );
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
                parameters.analysisRegistry,
                parameters.client
            )
        );
    }

    public void onIndexModule(IndexModule indexModule) {
        if (SparseSettings.IS_SPARSE_INDEX_SETTING.get(indexModule.getSettings())) {
            indexModule.addIndexEventListener(new SparseIndexEventListener());
        }
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        return new BreakerSettings(
            NEURAL_CIRCUIT_BREAKER_NAME,
            NEURAL_CIRCUIT_BREAKER_LIMIT.get(settings).getBytes(),
            NEURAL_CIRCUIT_BREAKER_OVERHEAD.get(settings),
            CircuitBreaker.Type.MEMORY,
            CircuitBreaker.Durability.PERMANENT
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        CircuitBreakerManager.setCircuitBreaker(circuitBreaker);
        MemoryUsageManager.getInstance().setLimitAndOverhead(new ByteSizeValue(circuitBreaker.getLimit()), circuitBreaker.getOverhead());
    }

    @Override
    public List<QueryCollectorContextSpecFactory> getCollectorContextSpecFactories() {
        return List.of(new HybridQueryCollectorContextSpecFactory());
    }
}
