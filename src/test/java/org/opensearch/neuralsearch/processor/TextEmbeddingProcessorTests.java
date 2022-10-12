/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.opensearch.client.Client;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.test.OpenSearchTestCase;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TextEmbeddingProcessorTests extends OpenSearchTestCase {

    private final TextEmbeddingProcessor.Factory factory = new TextEmbeddingProcessor.Factory(mock(Client.class));

    private static final String processorTag = "mockTag";

    private static final String description = "mockDescription";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    private static final String PLAIN_CONFIG_PATH = "processor/PlainStringConfigurationForTextEmbeddingProcessor.json";
    private static final String PLAIN_DATA_PATH = "processor/PlainStringIngestionDocument.json";

    private TextEmbeddingProcessor createInstance() throws Exception {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        return factory.create(registry, processorTag, description, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_TextEmbeddingProcessConstructor_config_empty() throws Exception {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put(null, "key1Mapped");
        fieldMap.put("key2", "key2Mapped");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        TextEmbeddingProcessor processor = factory.create(registry, processorTag, description, config);
        assertNull(processor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_TextEmbeddingProcessConstructor_config_error() throws Exception {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        TextEmbeddingProcessor processor = factory.create(registry, processorTag, description, config);
        assertNull(processor);
    }

    public void test_execute() throws Exception {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    public void test_execute_List_type() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", list1);
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_execute_List_type_exception() throws Exception {
        List<String> list1 = ImmutableList.of("", "test2", "test3");
        List<Integer> list2 = ImmutableList.of(1, 2, 3);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", list1);
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    public void test_execute_map_type() throws Exception {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test4", "test5");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_execute_map_type_exception() throws Exception {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, Double> map2 = ImmutableMap.of("test3", 209.3D);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    public void test_execute_hybrid_type() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2");
        Map<String, List<String>> map1 = ImmutableMap.of("test3", list1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", map1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_execute_text_type_exception() throws Exception {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", 100);
        sourceAndMetadata.put("key2", 100.232D);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key1");
    }

    public void test_getType() throws Exception {
        TextEmbeddingProcessor processor = createInstance();
        assert processor.getType().equals(TextEmbeddingProcessor.TYPE);
    }

    public void test_responseConsumer() throws Exception {
        Map<String, Object> config = createConfigMap(PLAIN_CONFIG_PATH);
        IngestDocument ingestDocument = createIngestDocument(PLAIN_DATA_PATH);
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildKnnMap(ingestDocument, config);

        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.responseConsumer(ingestDocument, knnMap).accept(modelTensorList);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
    }

    public void test_exceptionConsumer() throws Exception {
        Map<String, Object> config = createConfigMap(PLAIN_CONFIG_PATH);
        IngestDocument ingestDocument = createIngestDocument(PLAIN_DATA_PATH);
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        processor.exceptionConsumer().accept(new Exception("Connection timeout!"));
        assertEquals(6, ingestDocument.getSourceAndMetadata().size());
    }

    public void test_buildVectorOutput_plain_string() throws Exception {
        Map<String, Object> config = createConfigMap(PLAIN_CONFIG_PATH);
        IngestDocument ingestDocument = createIngestDocument(PLAIN_DATA_PATH);
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildKnnMap(ingestDocument, config);

        // To assert the order is not changed between config map and generated map.
        List<Object> configValueList = new LinkedList<>(config.values());
        List<String> knnKeyList = new LinkedList<>(knnMap.keySet());
        assertEquals(configValueList.size(), knnKeyList.size());
        assertEquals(knnKeyList.get(0), configValueList.get(0).toString());
        int lastIndex = knnKeyList.size() - 1;
        assertEquals(knnKeyList.get(lastIndex), configValueList.get(lastIndex).toString());

        List<List<Float>> modelTensorList = createMockVectorResult();
        Map<String, Object> result = processor.buildVectorOutput(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        String resultTxt = objectMapper.writeValueAsString(result);
        System.out.println(resultTxt);
        String expectedResult =
            "{\"oriKey6_knn\":[{\"knn\":[1.234,2.354]},{\"knn\":[3.234,4.354]}],\"oriKey5_knn\":[5.234,6.354],\"oriKey4_knn\":[7.234,8.354],\"oriKey3_knn\":[9.234,10.354],\"oriKey2_knn\":[11.234,12.354],\"oriKey1_knn\":[13.234,14.354]}";
        assertEquals(expectedResult, resultTxt);
    }

    public void test_buildVectorOutput_nested_map() throws Exception {
        Map<String, Object> config = createConfigMap("processor/NestedMapConfigurationForTextEmbeddingProcessor.json");
        IngestDocument ingestDocument = createIngestDocument("processor/NestedMapIngestionDocument.json");
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildKnnMap(ingestDocument, config);

        List<List<Float>> modelTensorList = createMockVectorResult();
        Map<String, Object> result = processor.buildVectorOutput(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        String resultTxt = objectMapper.writeValueAsString(result);
        System.out.println(resultTxt);
        String expectedResult =
            "{\"favorites\":{\"favorite.movie\":[\"The Matrix\",null,\"The Lord of the Rings\"],\"favorite.games\":{\"adventure\":{\"with.action\":[\"jojo world\",null],\"with.reaction\":\"overwatch\",\"with.action.knn\":[{\"knn\":[5.234,6.354]}],\"with.reaction.knn\":[7.234,8.354]},\"puzzle\":{\"maze\":\"zelda\",\"card\":\"hearthstone\",\"maze.knn\":[9.234,10.354],\"card.knn\":[11.234,12.354]}},\"favorite.songs\":\"In The Name Of Father\",\"favorite.movie.knn\":[{\"knn\":[1.234,2.354]},{\"knn\":[3.234,4.354]}],\"favorite.songs.knn\":[13.234,14.354]}}";
        assertEquals(expectedResult, objectMapper.writeValueAsString(ingestDocument.getSourceAndMetadata()));
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

    private TextEmbeddingProcessor createInstanceWithNestedMapConfiguration(Map<String, Object> fieldMap) throws Exception {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        return factory.create(registry, processorTag, description, config);
    }

    private Map<String, Object> createConfigMap(String configFile) throws IOException {
        String configuration = FileUtils.readFileToString(new File(classLoader.getResource(configFile).getFile()), "utf-8");
        return objectMapper.readValue(configuration, new TypeReference<HashMap<String, Object>>() {
        });
    }

    private IngestDocument createIngestDocument(String ingestFile) throws IOException {
        String sourceAndMetadataMap = FileUtils.readFileToString(new File(classLoader.getResource(ingestFile).getFile()), "utf-8");
        Map<String, Object> sourceAndMetadata = objectMapper.readValue(sourceAndMetadataMap, new TypeReference<HashMap<String, Object>>() {
        });
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }
}
