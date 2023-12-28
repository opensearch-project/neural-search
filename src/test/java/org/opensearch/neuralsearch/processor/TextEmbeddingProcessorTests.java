/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class TextEmbeddingProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment env;

    @InjectMocks
    private TextEmbeddingProcessorFactory textEmbeddingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(env.settings()).thenReturn(settings);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstance(List<List<Float>> vector) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        return textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapError_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put(null, "key1Mapped");
        fieldMap.put("key2", "key2Mapped");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        try {
            textEmbeddingProcessorFactory.create(registry, TextEmbeddingProcessor.TYPE, DESCRIPTION, config);
        } catch (IllegalArgumentException e) {
            assertEquals("Unable to create the processor as field_map has invalid key or value", e.getMessage());
        }
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapEmpty_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        try {
            textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        } catch (OpenSearchParseException e) {
            assertEquals("[field_map] required property is missing", e.getMessage());
        }
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    @SneakyThrows
    public void testExecute_whenInferenceThrowInterruptedException_throwRuntimeException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(accessor, env);

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        doThrow(new RuntimeException()).when(accessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    @SneakyThrows
    public void testExecute_whenInferenceTextListEmpty_SuccessWithoutEmbedding() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(accessor, env);

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        doThrow(new RuntimeException()).when(accessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_withListTypeInput_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", list1);
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(6));

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_SimpleTypeWithEmptyStringValue_throwIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "    ");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasEmptyStringValue_throwIllegalArgumentException() {
        List<String> list1 = ImmutableList.of("", "test2", "test3");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", list1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNonStringValue_throwIllegalArgumentException() {
        List<Integer> list2 = ImmutableList.of(1, 2, 3);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNull_throwIllegalArgumentException() {
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add(null);
        list.add("world");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", list);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_withMapTypeInput_successful() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test4", "test5");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());

    }

    public void testExecute_mapHasNonStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, Double> map2 = ImmutableMap.of("test3", 209.3D);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapHasEmptyStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test3", "   ");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapDepthReachLimit_throwIllegalArgumentException() {
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> 1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "hello world");
        sourceAndMetadata.put("key2", ret);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));

        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    private Map<String, Object> createMaxDepthLimitExceedMap(Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > 21) {
            return null;
        }
        Map<String, Object> innerMap = new HashMap<>();
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> maxDepth + 1);
        if (ret == null) return innerMap;
        innerMap.put("hello", ret);
        return innerMap;
    }

    public void testExecute_hybridTypeInput_successful() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2");
        Map<String, List<String>> map1 = ImmutableMap.of("test3", list1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", map1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key2");
    }

    public void testExecute_simpleTypeInputWithNonStringValue_handleIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", 100);
        sourceAndMetadata.put("key2", 100.232D);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testGetType_successful() {
        TextEmbeddingProcessor processor = createInstance(createMockVectorWithLength(2));
        assert processor.getType().equals(TextEmbeddingProcessor.TYPE);
    }

    public void testProcessResponse_successful() throws Exception {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithProcessorKeyAndOriginalValue(ingestDocument);

        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
    }

    @SneakyThrows
    public void testBuildVectorOutput_withPlainStringValue_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithProcessorKeyAndOriginalValue(ingestDocument);

        // To assert the order is not changed between config map and generated map.
        List<Object> configValueList = new LinkedList<>(config.values());
        List<String> knnKeyList = new LinkedList<>(knnMap.keySet());
        assertEquals(configValueList.size(), knnKeyList.size());
        assertEquals(knnKeyList.get(0), configValueList.get(0).toString());
        int lastIndex = knnKeyList.size() - 1;
        assertEquals(knnKeyList.get(lastIndex), configValueList.get(lastIndex).toString());

        List<List<Float>> modelTensorList = createMockVectorResult();
        Map<String, Object> result = processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        assertTrue(result.containsKey("oriKey1_knn"));
        assertTrue(result.containsKey("oriKey2_knn"));
        assertTrue(result.containsKey("oriKey3_knn"));
        assertTrue(result.containsKey("oriKey4_knn"));
        assertTrue(result.containsKey("oriKey5_knn"));
        assertTrue(result.containsKey("oriKey6_knn"));
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedMap_successful() {
        Map<String, Object> config = createNestedMapConfiguration();
        IngestDocument ingestDocument = createNestedMapIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        Map<String, Object> favoritesMap = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("favorites");
        assertNotNull(favoritesMap);
        Map<String, Object> favoriteGames = (Map<String, Object>) favoritesMap.get("favorite.games");
        assertNotNull(favoriteGames);
        Map<String, Object> adventure = (Map<String, Object>) favoriteGames.get("adventure");
        Object actionGamesKnn = adventure.get("with.action.knn");
        assertNotNull(actionGamesKnn);
    }

    public void test_updateDocument_appendVectorFieldsToDocument_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithProcessorKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);

        List<List<Float>> modelTensorList1 = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList1);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
        assertEquals(2, ((List<?>) ingestDocument.getSourceAndMetadata().get("oriKey6_knn")).size());
    }

    private List<List<Float>> createMockVectorResult() {
        List<List<Float>> modelTensorList = new ArrayList<>();
        List<Float> number1 = ImmutableList.of(1.234f, 2.354f);
        List<Float> number2 = ImmutableList.of(3.234f, 4.354f);
        List<Float> number3 = ImmutableList.of(5.234f, 6.354f);
        List<Float> number4 = ImmutableList.of(7.234f, 8.354f);
        List<Float> number5 = ImmutableList.of(9.234f, 10.354f);
        List<Float> number6 = ImmutableList.of(11.234f, 12.354f);
        List<Float> number7 = ImmutableList.of(13.234f, 14.354f);
        modelTensorList.add(number1);
        modelTensorList.add(number2);
        modelTensorList.add(number3);
        modelTensorList.add(number4);
        modelTensorList.add(number5);
        modelTensorList.add(number6);
        modelTensorList.add(number7);
        return modelTensorList;
    }

    private List<List<Float>> createMockVectorWithLength(int size) {
        float suffix = .234f;
        List<List<Float>> result = new ArrayList<>();
        for (int i = 0; i < size * 2;) {
            List<Float> number = new ArrayList<>();
            number.add(i++ + suffix);
            number.add(i++ + suffix);
            result.add(number);
        }
        return result;
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedMapConfiguration(Map<String, Object> fieldMap) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        return textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    private Map<String, Object> createPlainStringConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("oriKey1", "oriKey1_knn");
        config.put("oriKey2", "oriKey2_knn");
        config.put("oriKey3", "oriKey3_knn");
        config.put("oriKey4", "oriKey4_knn");
        config.put("oriKey5", "oriKey5_knn");
        config.put("oriKey6", "oriKey6_knn");
        return config;
    }

    private Map<String, Object> createNestedMapConfiguration() {
        Map<String, Object> adventureGames = new HashMap<>();
        adventureGames.put("with.action", "with.action.knn");
        adventureGames.put("with.reaction", "with.reaction.knn");
        Map<String, Object> puzzleGames = new HashMap<>();
        puzzleGames.put("maze", "maze.knn");
        puzzleGames.put("card", "card.knn");
        Map<String, Object> favoriteGames = new HashMap<>();
        favoriteGames.put("adventure", adventureGames);
        favoriteGames.put("puzzle", puzzleGames);
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite.movie", "favorite.movie.knn");
        favorite.put("favorite.games", favoriteGames);
        favorite.put("favorite.songs", "favorite.songs.knn");
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return result;
    }

    private IngestDocument createPlainIngestDocument() {
        Map<String, Object> result = new HashMap<>();
        result.put("oriKey1", "oriValue1");
        result.put("oriKey2", "oriValue2");
        result.put("oriKey3", "oriValue3");
        result.put("oriKey4", "oriValue4");
        result.put("oriKey5", "oriValue5");
        result.put("oriKey6", ImmutableList.of("oriValue6", "oriValue7"));
        return new IngestDocument(result, new HashMap<>());
    }

    private IngestDocument createNestedMapIngestDocument() {
        Map<String, Object> adventureGames = new HashMap<>();
        List<String> actionGames = new ArrayList<>();
        actionGames.add("jojo world");
        actionGames.add(null);
        adventureGames.put("with.action", actionGames);
        adventureGames.put("with.reaction", "overwatch");
        Map<String, Object> puzzleGames = new HashMap<>();
        puzzleGames.put("maze", "zelda");
        puzzleGames.put("card", "hearthstone");
        Map<String, Object> favoriteGames = new HashMap<>();
        favoriteGames.put("adventure", adventureGames);
        favoriteGames.put("puzzle", puzzleGames);
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite.movie", "favorite.movie.knn");
        favorite.put("favorite.games", favoriteGames);
        favorite.put("favorite.songs", "In The Name Of Father");
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return new IngestDocument(result, new HashMap<>());
    }
}
