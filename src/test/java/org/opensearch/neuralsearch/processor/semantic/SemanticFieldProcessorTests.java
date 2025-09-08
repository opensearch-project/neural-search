/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.semantic;

import lombok.NonNull;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.VersionType;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SKIP_EXISTING_EMBEDDING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessorTests.getAnalysisRegistry;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.transport.client.OpenSearchClient;

public class SemanticFieldProcessorTests extends OpenSearchTestCase {
    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    private AnalysisRegistry analysisRegistry;
    @Mock
    private Environment environment;
    @Mock
    private ClusterService clusterService;
    @Mock
    private OpenSearchClient openSearchClient;
    private Chunker chunker;
    private final ClassLoader classLoader = this.getClass().getClassLoader();

    private Map<String, Map<String, Object>> pathToFieldConfigMap;

    private SemanticFieldProcessor semanticFieldProcessor;
    private final String DUMMY_MODEL_ID_1 = "dummy_model_id_1";
    private final String DUMMY_MODEL_ID_2 = "dummy_model_id_2";
    private final String FIELD_NAME_PRODUCTS = "products";
    private final String FIELD_NAME_PRODUCT_DESCRIPTION = "product_description";
    private final String FIELD_NAME_GEO_DATA = "geo_data";

    private MLModel textEmbeddingModel;
    private MLModel sparseEmbeddingModel;

    @Before
    public void setup() {
        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();

        MockitoAnnotations.openMocks(this);
        // mock env
        final Settings settings = Settings.builder()
            .put("index.mapping.depth.limit", 20)
            .put("index.analyze.max_token_count", 10000)
            .put("index.number_of_shards", 1)
            .build();
        when(environment.settings()).thenReturn(settings);
        // mock cluster
        final Metadata metadata = mock(Metadata.class);
        final ClusterState clusterState = mock(ClusterState.class);
        clusterService = mock(ClusterService.class);
        when(metadata.index(anyString())).thenReturn(null);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(clusterState);
        // mock analysisRegistry
        analysisRegistry = getAnalysisRegistry();

        // two semantic fields with different model ids
        // one field enable the chunking and one field disable the chunking
        pathToFieldConfigMap = Map.of(
            FIELD_NAME_PRODUCTS + PATH_SEPARATOR + FIELD_NAME_PRODUCT_DESCRIPTION,
            Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE, MODEL_ID, DUMMY_MODEL_ID_1, CHUNKING, true),
            FIELD_NAME_GEO_DATA,
            Map.of(
                TYPE,
                SemanticFieldMapper.CONTENT_TYPE,
                MODEL_ID,
                DUMMY_MODEL_ID_2,
                SPARSE_ENCODING_CONFIG,
                Map.of(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue(), PruneUtils.PRUNE_RATIO_FIELD, 0.5)
            )
        );

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{\"space_type\":\"l2\"}";
        final TextEmbeddingModelConfig textEmbeddingModelConfig = TextEmbeddingModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType("modelType")
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        textEmbeddingModel = MLModel.builder()
            .algorithm(FunctionName.TEXT_EMBEDDING)
            .modelConfig(textEmbeddingModelConfig)
            .name(FunctionName.TEXT_EMBEDDING.name())
            .build();

        sparseEmbeddingModel = MLModel.builder().algorithm(FunctionName.SPARSE_ENCODING).name(FunctionName.SPARSE_ENCODING.name()).build();

        final Map<String, Object> chunkerParameters = new HashMap<>();
        chunkerParameters.put(FixedTokenLengthChunker.TOKEN_LIMIT_FIELD, 50);
        chunkerParameters.put(FixedTokenLengthChunker.OVERLAP_RATE_FIELD, 0.2);
        chunkerParameters.put(FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        chunker = ChunkerFactory.create(FixedTokenLengthChunker.ALGORITHM_NAME, chunkerParameters);

        semanticFieldProcessor = new SemanticFieldProcessor(
            "tag",
            "description",
            1,
            pathToFieldConfigMap,
            mlCommonsClientAccessor,
            environment,
            clusterService,
            chunker,
            analysisRegistry,
            openSearchClient
        );
    }

    public void testExecute_whenNoSemanticField_thenDoNothing() {
        // Scenario: No semantic fields in the ingest document
        final IngestDocument ingestDocument = new IngestDocument("index", "1", "routing", 1L, VersionType.INTERNAL, new HashMap<>());

        // Call the method
        semanticFieldProcessor.execute(ingestDocument, (doc, e) -> {
            assertNull("No error should occur", e);
            assertNotNull("Ingest document should be passed unchanged", doc);
        });
    }

    public void testExecute_whenValidDoc_thenIngestDocSuccessfully() throws URISyntaxException, IOException {
        // prepare ingest doc
        final Map<String, Object> ingestDocSource = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocument ingestDocument = new IngestDocument("index", "1", "routing", 1L, VersionType.INTERNAL, ingestDocSource);

        mockGetModelAndInferenceAPI();

        // Call the method
        semanticFieldProcessor.execute(ingestDocument, (doc, e) -> {
            assertNull("No error should occur", e);
            // verify
            final Map<String, Object> expectedIngestedDoc;
            try {
                expectedIngestedDoc = readExpectedDocFromFile("processor/semantic/ingested_doc1.json");
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            org.assertj.core.api.Assertions.assertThat(ingestDocument.getSourceAndMetadata()).isEqualTo(expectedIngestedDoc);

            verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
            verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
            verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), isNull(), any());
        });

        // prepare ingest doc 2 to test we can reuse the model config fetched before
        final Map<String, Object> ingestDocSource2 = readDocSourceFromFile("processor/semantic/ingest_doc2.json");
        final IngestDocument ingestDocument2 = new IngestDocument("index", "2", "routing", 1L, VersionType.INTERNAL, ingestDocSource2);

        // call method
        semanticFieldProcessor.execute(ingestDocument2, (doc, e) -> {
            assertNull("No error should occur", e);

            // No more invocation of getModel API but still invoke inference function.
            verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
            verify(mlCommonsClientAccessor, times(2)).inferenceSentences(any(), any());
            verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), isNull(), any());
        });
    }

    public void testExecute_whenValidDocReuseEmbedding_thenIngestDocSuccessfully() throws URISyntaxException, IOException {
        // enable reuse exist embedding
        final Map<String, Map<String, Object>> pathToFieldConfigMap = Map.of(
            FIELD_NAME_PRODUCTS + PATH_SEPARATOR + FIELD_NAME_PRODUCT_DESCRIPTION,
            Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE, MODEL_ID, DUMMY_MODEL_ID_1, CHUNKING, true, SKIP_EXISTING_EMBEDDING, true),
            FIELD_NAME_GEO_DATA,
            Map.of(
                TYPE,
                SemanticFieldMapper.CONTENT_TYPE,
                MODEL_ID,
                DUMMY_MODEL_ID_2,
                SPARSE_ENCODING_CONFIG,
                Map.of(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue(), PruneUtils.PRUNE_RATIO_FIELD, 0.5)
            )
        );
        semanticFieldProcessor = new SemanticFieldProcessor(
            "tag",
            "description",
            1,
            pathToFieldConfigMap,
            mlCommonsClientAccessor,
            environment,
            clusterService,
            chunker,
            analysisRegistry,
            openSearchClient
        );
        // prepare ingest doc
        final Map<String, Object> ingestDocSource = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocument ingestDocument = new IngestDocument("index", "1", "routing", 1L, VersionType.INTERNAL, ingestDocSource);

        mockGetModelAndInferenceAPI();

        // first time doc not exist
        final MultiGetResponse multiGetResponse = Mockito.mock(MultiGetResponse.class);
        final MultiGetItemResponse multiGetItemResponse = Mockito.mock(MultiGetItemResponse.class);
        final GetResponse getResponse = Mockito.mock(GetResponse.class);
        when(multiGetResponse.getResponses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponse });
        when(multiGetItemResponse.getId()).thenReturn("1");
        when(multiGetItemResponse.getResponse()).thenReturn(getResponse);
        when(getResponse.isExists()).thenReturn(false);
        doAnswer(invocationOnMock -> {
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(multiGetResponse);
            return null;
        }).when(openSearchClient).execute(any(), any(), any());

        // prepare data for verify
        final Map<String, Object> expectedIngestedDoc;
        try {
            expectedIngestedDoc = readExpectedDocFromFile("processor/semantic/ingested_doc1.json");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        // Call the method
        semanticFieldProcessor.execute(ingestDocument, (doc, e) -> {
            assertNull("No error should occur", e);

            org.assertj.core.api.Assertions.assertThat(ingestDocument.getSourceAndMetadata()).isEqualTo(expectedIngestedDoc);

            verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
            verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
            verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), isNull(), any());
        });

        // ingest the doc again to verify it should reuse the existing embedding
        final Map<String, Object> ingestDocSourceSame = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocument ingestDocumentSame = new IngestDocument(
            "index",
            "1",
            "routing",
            1L,
            VersionType.INTERNAL,
            ingestDocSourceSame
        );

        // second time doc exist
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsMap()).thenReturn(ingestDocument.getSourceAndMetadata());

        // call method
        semanticFieldProcessor.execute(ingestDocumentSame, (doc, e) -> {
            assertNull("No error should occur", e);

            // verify
            org.assertj.core.api.Assertions.assertThat(ingestDocumentSame.getSourceAndMetadata()).isEqualTo(expectedIngestedDoc);

            // No more invocation of getModel API
            verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
            // No more inference for the dense model since we reuse the existing embedding for it
            verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
            // Still inference for the sparse model since we don't reuse the existing embedding for it
            verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), isNull(), any());
        });
    }

    public void testExecute_whenInvalidDocNotAString_thenException() throws URISyntaxException, IOException {
        // prepare ingest doc
        final Map<String, Object> ingestDocSource = readDocSourceFromFile("processor/semantic/invalid_ingest_doc.json");
        final IngestDocument ingestDocument = new IngestDocument("index", "1", "routing", 1L, VersionType.INTERNAL, ingestDocSource);

        // Call the method
        semanticFieldProcessor.execute(ingestDocument, (doc, e) -> {
            assertEquals(
                "Expect the semantic field at path: products.product_description to be a string but found: class java.util.HashMap.",
                e.getMessage()
            );
        });
    }

    public void testSubBatchExecute_whenNoDoc_thenDoNothing() {

        // mock handler
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(), handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());
        final List<IngestDocumentWrapper> ingestedDocs = handlerCaptor.getValue();

        // verify no change
        assertTrue(ingestedDocs.isEmpty());
    }

    public void testSubBatchExecute_whenNoSemanticField_thenDoNothing() {
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocWrapper("1", new HashMap<>());
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocWrapper("1", new HashMap<>());
        final List<IngestDocumentWrapper> ingestDocumentWrappers = List.of(ingestDocumentWrapper1, ingestDocumentWrapper2);

        // mock handler
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);

        // Call the method
        semanticFieldProcessor.subBatchExecute(ingestDocumentWrappers, handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());
        final List<IngestDocumentWrapper> ingestedDocs = handlerCaptor.getValue();

        // verify no change
        org.assertj.core.api.Assertions.assertThat(ingestedDocs).isEqualTo(ingestDocumentWrappers);
    }

    @SuppressWarnings("unchecked")
    public void testSubBatchExecute_whenValidDocs_thenIngestDocsSuccessfully() throws URISyntaxException, IOException {
        // prepare ingest doc 1
        final Map<String, Object> ingestDocSource1 = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocWrapper("1", ingestDocSource1);
        // prepare ingest doc 2
        final Map<String, Object> ingestDocSource2 = readDocSourceFromFile("processor/semantic/ingest_doc2.json");
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocWrapper("2", ingestDocSource2);

        // mock handler
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);

        mockGetModelAndInferenceAPI();

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(ingestDocumentWrapper1, ingestDocumentWrapper2), handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());

        verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), isNull(), any());

        final List<IngestDocumentWrapper> ingestedDocs = handlerCaptor.getValue();

        // verify
        final Map<String, Object> expectedIngestedDoc1 = readExpectedDocFromFile("processor/semantic/ingested_doc1.json");
        org.assertj.core.api.Assertions.assertThat(ingestedDocs.get(0).getIngestDocument().getSourceAndMetadata())
            .isEqualTo(expectedIngestedDoc1);

        final Map<String, Object> expectedIngestedDoc2 = readExpectedDocFromFile("processor/semantic/ingested_doc2.json");
        org.assertj.core.api.Assertions.assertThat(ingestedDocs.get(1).getIngestDocument().getSourceAndMetadata())
            .isEqualTo(expectedIngestedDoc2);

    }

    @SuppressWarnings("unchecked")
    public void testSubBatchExecute_whenValidDocsSkipExistingEmbedding_thenIngestDocsSuccessfully() throws URISyntaxException, IOException {
        // enable reuse exist embedding
        final Map<String, Map<String, Object>> pathToFieldConfigMap = Map.of(
            FIELD_NAME_PRODUCTS + PATH_SEPARATOR + FIELD_NAME_PRODUCT_DESCRIPTION,
            Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE, MODEL_ID, DUMMY_MODEL_ID_1, CHUNKING, true, SKIP_EXISTING_EMBEDDING, true),
            FIELD_NAME_GEO_DATA,
            Map.of(
                TYPE,
                SemanticFieldMapper.CONTENT_TYPE,
                MODEL_ID,
                DUMMY_MODEL_ID_2,
                SPARSE_ENCODING_CONFIG,
                Map.of(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue(), PruneUtils.PRUNE_RATIO_FIELD, 0.5),
                SKIP_EXISTING_EMBEDDING,
                true
            )
        );
        semanticFieldProcessor = new SemanticFieldProcessor(
            "tag",
            "description",
            1,
            pathToFieldConfigMap,
            mlCommonsClientAccessor,
            environment,
            clusterService,
            chunker,
            analysisRegistry,
            openSearchClient
        );
        // prepare ingest doc 1
        final Map<String, Object> ingestDocSource1 = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocWrapper("1", ingestDocSource1);
        // prepare ingest doc 2
        final Map<String, Object> ingestDocSource2 = readDocSourceFromFile("processor/semantic/ingest_doc2.json");
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocWrapper("2", ingestDocSource2);

        // mock handler
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);

        mockGetModelAndInferenceAPI();
        // first time docs not exist
        final MultiGetResponse multiGetResponse = Mockito.mock(MultiGetResponse.class);
        final MultiGetItemResponse multiGetItemResponse1 = Mockito.mock(MultiGetItemResponse.class);
        final GetResponse getResponse1 = Mockito.mock(GetResponse.class);
        final MultiGetItemResponse multiGetItemResponse2 = Mockito.mock(MultiGetItemResponse.class);
        final GetResponse getResponse2 = Mockito.mock(GetResponse.class);
        when(multiGetResponse.getResponses()).thenReturn(new MultiGetItemResponse[] { multiGetItemResponse1, multiGetItemResponse2 });
        when(multiGetItemResponse1.getId()).thenReturn("1");
        when(multiGetItemResponse1.getResponse()).thenReturn(getResponse1);
        when(getResponse1.isExists()).thenReturn(false);
        when(multiGetItemResponse2.getId()).thenReturn("2");
        when(multiGetItemResponse2.getResponse()).thenReturn(getResponse2);
        when(getResponse2.isExists()).thenReturn(false);
        doAnswer(invocationOnMock -> {
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(multiGetResponse);
            return null;
        }).when(openSearchClient).execute(any(), any(), any());

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(ingestDocumentWrapper1, ingestDocumentWrapper2), handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());

        verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), isNull(), any());

        final List<IngestDocumentWrapper> ingestedDocs = handlerCaptor.getValue();

        // verify
        final Map<String, Object> expectedIngestedDoc1 = readExpectedDocFromFile("processor/semantic/ingested_doc1.json");
        org.assertj.core.api.Assertions.assertThat(ingestedDocs.get(0).getIngestDocument().getSourceAndMetadata())
            .isEqualTo(expectedIngestedDoc1);

        final Map<String, Object> expectedIngestedDoc2 = readExpectedDocFromFile("processor/semantic/ingested_doc2.json");
        org.assertj.core.api.Assertions.assertThat(ingestedDocs.get(1).getIngestDocument().getSourceAndMetadata())
            .isEqualTo(expectedIngestedDoc2);

        // prepare ingest doc 1 same
        final Map<String, Object> ingestDocSource1Same = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocumentWrapper ingestDocumentWrapper1Same = createIngestDocWrapper("1", ingestDocSource1Same);
        // prepare ingest doc 2 modified
        final Map<String, Object> ingestDocSource2Modified = readDocSourceFromFile("processor/semantic/ingest_doc2.json");
        ingestDocSource2Modified.put("geo_data", "updated dummy_geo_data");
        final IngestDocumentWrapper ingestDocumentWrapper2Modified = createIngestDocWrapper("2", ingestDocSource2Modified);

        // second time doc exist
        when(getResponse1.isExists()).thenReturn(true);
        when(getResponse1.getSourceAsMap()).thenReturn(ingestedDocs.get(0).getIngestDocument().getSourceAndMetadata());
        when(getResponse2.isExists()).thenReturn(true);
        when(getResponse2.getSourceAsMap()).thenReturn(ingestedDocs.get(1).getIngestDocument().getSourceAndMetadata());

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(ingestDocumentWrapper1Same, ingestDocumentWrapper2Modified), handler);

        // reuse model info
        verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
        // reuse dense embedding since the original value is not changed
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
        // generate sparse embedding again since the original value is updated
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), isNull(), any());
    }

    public void testSubBatchExecute_whenModelNotFound_thenAddExceptionToDocProperly() throws URISyntaxException, IOException {
        // prepare ingest doc 1
        final Map<String, Object> ingestDocSource1 = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocWrapper("1", ingestDocSource1);
        // prepare ingest doc 2
        final Exception mockException = mock(Exception.class);
        final Map<String, Object> ingestDocSource2 = readDocSourceFromFile("processor/semantic/ingest_doc2.json");
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocWrapper("2", ingestDocSource2, mockException);

        // mock handler
        final Exception mockModelNotFoundException = mock(Exception.class);
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);
        doAnswer(invocationOnMock -> {
            final Consumer<Exception> onFailure = invocationOnMock.getArgument(2);
            onFailure.accept(mockModelNotFoundException);
            return null;
        }).when(mlCommonsClientAccessor).getModels(any(), any(), any());

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(ingestDocumentWrapper1, ingestDocumentWrapper2), handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());

        verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
        verifyNoMoreInteractions(mlCommonsClientAccessor);

        assertEquals(mockModelNotFoundException, handlerCaptor.getValue().get(0).getException());
        assertEquals(mockException, handlerCaptor.getValue().get(1).getException());
    }

    public void testSubBatchExecute_whenInferencePartialFail_thenAddExceptionToDocProperly() throws URISyntaxException, IOException {
        // prepare ingest doc 1
        final Map<String, Object> ingestDocSource1 = readDocSourceFromFile("processor/semantic/ingest_doc1.json");
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocWrapper("1", ingestDocSource1);
        // prepare ingest doc 2
        final Map<String, Object> ingestDocSource2 = readDocSourceFromFile("processor/semantic/ingest_doc3.json");
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocWrapper("2", ingestDocSource2);

        // mock handler
        final Consumer<List<IngestDocumentWrapper>> handler = mock(Consumer.class);

        // mock get model API
        doAnswer(invocationOnMock -> {
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Map.of(DUMMY_MODEL_ID_1, textEmbeddingModel, DUMMY_MODEL_ID_2, sparseEmbeddingModel));
            return null;
        }).when(mlCommonsClientAccessor).getModels(any(), any(), any());

        // mock generate embedding for dense model success
        final String exceptionMsg = "Failed to inference by dense model.";
        doAnswer(invocationOnMock -> {
            final TextInferenceRequest textInferenceRequest = invocationOnMock.getArgument(0);
            final String modelId = textInferenceRequest.getModelId();

            final ActionListener<List<List<Number>>> listener = invocationOnMock.getArgument(1);
            assertEquals("dense model should be the model id 1", DUMMY_MODEL_ID_1, modelId);
            listener.onFailure(new RuntimeException(exceptionMsg));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(any(), any());

        // mock generate embedding for sparse model success
        doAnswer(invocationOnMock -> {
            final TextInferenceRequest textInferenceRequest = invocationOnMock.getArgument(0);
            final String modelId = textInferenceRequest.getModelId();
            final ActionListener<List<Map<String, ?>>> listener = invocationOnMock.getArgument(2);
            assertEquals("sparse model should be the model id 2", DUMMY_MODEL_ID_2, modelId);
            listener.onResponse(List.of(Map.of("response", List.of(Map.of("high score token", 1.0)))));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(any(), isNull(), any());

        // Call the method
        semanticFieldProcessor.subBatchExecute(List.of(ingestDocumentWrapper1, ingestDocumentWrapper2), handler);

        // Capture the final invocation of handler.accept
        final ArgumentCaptor<List<IngestDocumentWrapper>> handlerCaptor = ArgumentCaptor.forClass(List.class);
        verify(handler).accept(handlerCaptor.capture());

        verify(mlCommonsClientAccessor, times(1)).getModels(any(), any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(any(), any());
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), isNull(), any());

        final List<IngestDocumentWrapper> ingestedDocs = handlerCaptor.getValue();

        // verify
        assertTrue(ingestedDocs.get(0).getException().getMessage().contains(exceptionMsg));

        assertNull(ingestedDocs.get(1).getException());
        final Map<String, Object> expectedIngestedDoc1 = readExpectedDocFromFile("processor/semantic/ingested_doc3.json");

        org.assertj.core.api.Assertions.assertThat(ingestedDocs.get(1).getIngestDocument().getSourceAndMetadata())
            .isEqualTo(expectedIngestedDoc1);
    }

    private void mockGetModelAndInferenceAPI() {
        // mock get model API
        doAnswer(invocationOnMock -> {
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Map.of(DUMMY_MODEL_ID_1, textEmbeddingModel, DUMMY_MODEL_ID_2, sparseEmbeddingModel));
            return null;
        }).when(mlCommonsClientAccessor).getModels(any(), any(), any());

        // mock generate embedding for dense model
        doAnswer(invocationOnMock -> {
            final TextInferenceRequest textInferenceRequest = invocationOnMock.getArgument(0);
            final String modelId = textInferenceRequest.getModelId();
            final ActionListener<List<List<Number>>> listener = invocationOnMock.getArgument(1);
            assertEquals("dense model should be the model id 1", DUMMY_MODEL_ID_1, modelId);
            listener.onResponse(List.of(List.of(1.0), List.of(2.0), List.of(3.0)));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(any(), any());

        // mock generate embedding for sparse model
        doAnswer(invocationOnMock -> {
            final TextInferenceRequest textInferenceRequest = invocationOnMock.getArgument(0);
            final String modelId = textInferenceRequest.getModelId();
            final ActionListener<List<Map<String, ?>>> listener = invocationOnMock.getArgument(2);
            assertEquals("sparse model should be the model id 2", DUMMY_MODEL_ID_2, modelId);
            listener.onResponse(List.of(Map.of("response", List.of(Map.of("high score token", 1.0, "low score token", 0.1)))));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(any(), isNull(), any());

    }

    private Map<String, Object> readDocSourceFromFile(String filePath) throws URISyntaxException, IOException {
        final String docStr = Files.readString(Path.of(classLoader.getResource(filePath).toURI()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), docStr, false);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readExpectedDocFromFile(String filePath) throws URISyntaxException, IOException {
        final Map<String, Object> expectedIngestedDoc = readDocSourceFromFile(filePath);
        // manually add the _version
        expectedIngestedDoc.put("_version", Long.valueOf(expectedIngestedDoc.get("_version").toString()));
        // For sparse model we need to convert the double we define in the json file to float otherwise the compare will
        // fail.
        Map<String, Object> geoDataSemanticInfo = (Map<String, Object>) expectedIngestedDoc.get("geo_data_semantic_info");
        List<Map<String, Object>> chunks = (List<Map<String, Object>>) geoDataSemanticInfo.get("chunks");
        // If chunking is enabled when process each chunk
        if (chunks != null) {
            for (Map<String, Object> chunk : chunks) {
                Map<String, Double> embedding = (Map<String, Double>) chunk.get("embedding");
                chunk.put("embedding", convertToEmbeddingWithFloat(embedding));
            }
        } else {
            // If chunking is disabled then process the embedding directly
            Map<String, Double> embedding = (Map<String, Double>) geoDataSemanticInfo.get("embedding");
            geoDataSemanticInfo.put("embedding", convertToEmbeddingWithFloat(embedding));
        }

        return expectedIngestedDoc;
    }

    private Map<String, Float> convertToEmbeddingWithFloat(Map<String, Double> embeddingWithDouble) {
        final Map<String, Float> embeddingWithFloat = new HashMap<>();
        for (Map.Entry<String, Double> entry : embeddingWithDouble.entrySet()) {
            embeddingWithFloat.put(entry.getKey(), entry.getValue().floatValue());
        }
        return embeddingWithFloat;
    }

    private IngestDocumentWrapper createIngestDocWrapper(@NonNull final String id, @NonNull final Map<String, Object> source) {
        return createIngestDocWrapper(id, source, null);
    }

    private IngestDocumentWrapper createIngestDocWrapper(@NonNull final String id, @NonNull final Map<String, Object> source, Exception e) {
        final IngestDocument ingestDocument = new IngestDocument("index", id, "routing", 1L, VersionType.INTERNAL, source);
        return new IngestDocumentWrapper(1, 0, ingestDocument, e);
    }

}
