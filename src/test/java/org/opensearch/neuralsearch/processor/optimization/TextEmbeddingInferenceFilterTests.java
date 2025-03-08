/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextEmbeddingInferenceFilterTests extends OpenSearchTestCase {

    private Map<String, Object> sourceAndMetadataMap;
    private Map<String, Object> existingSourceAndMetadataMap;
    private TextEmbeddingInferenceFilter textEmbeddingInferenceFilter;
    private TextEmbeddingInferenceFilter nestedTextEmbeddingInferenceFilter;

    @Before
    public void setup() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("textField", "embeddingField");
        Map<String, Object> nestedFieldMap = new HashMap<>();
        nestedFieldMap.put("outerField", fieldMap);
        textEmbeddingInferenceFilter = new TextEmbeddingInferenceFilter(fieldMap);
        sourceAndMetadataMap = new HashMap<>();
        existingSourceAndMetadataMap = new HashMap<>();
        nestedTextEmbeddingInferenceFilter = new TextEmbeddingInferenceFilter(nestedFieldMap);
    }

    public void test_filterInferenceValue_TextUnchanged_ShouldCopyEmbedding() {
        String textPath = "textField";
        String embeddingPath = "embeddingField";
        String textValue = "Hello World";
        List<Double> embeddingValue = Arrays.asList(0.1, 0.2, 0.3);

        sourceAndMetadataMap.put(textPath, textValue);
        existingSourceAndMetadataMap.put(textPath, textValue);
        existingSourceAndMetadataMap.put(embeddingPath, embeddingValue);

        Object result = textEmbeddingInferenceFilter.filterInferenceValue(
            embeddingPath,
            textValue,
            sourceAndMetadataMap,
            existingSourceAndMetadataMap,
            -1
        );
        assertNull(result);
        assertEquals(embeddingValue, sourceAndMetadataMap.get(embeddingPath));
    }

    public void test_filterInferenceValue_TextChanged_ShouldNotCopyEmbedding() {
        String textPath = "textField";
        String embeddingPath = "embeddingField";
        String newText = "New Text";
        String oldText = "Old Text";
        List<Double> embeddingValue = Arrays.asList(0.1, 0.2, 0.3);

        sourceAndMetadataMap.put(textPath, newText);
        existingSourceAndMetadataMap.put(textPath, oldText);
        existingSourceAndMetadataMap.put(embeddingPath, embeddingValue);

        Object result = textEmbeddingInferenceFilter.filterInferenceValue(
            embeddingPath,
            newText,
            sourceAndMetadataMap,
            existingSourceAndMetadataMap,
            -1
        );

        assertEquals(newText, result);
        assertNull(sourceAndMetadataMap.get(embeddingPath));
    }

    public void test_filterInferenceValue_NoExistingEmbedding_ShouldNotCopy() {
        String textPath = "textField";
        String embeddingPath = "embeddingField";
        String textValue = "Hello World";

        sourceAndMetadataMap.put(textPath, textValue);
        existingSourceAndMetadataMap.put(textPath, textValue);
        existingSourceAndMetadataMap.put(embeddingPath, null);

        Object result = textEmbeddingInferenceFilter.filterInferenceValue(
            embeddingPath,
            textValue,
            sourceAndMetadataMap,
            existingSourceAndMetadataMap,
            -1
        );

        assertEquals(textValue, result);
        assertNull(sourceAndMetadataMap.get(embeddingPath));
    }

    public void test_filterInferenceValuesInList_ListUnchanged_ShouldCopyAllEmbeddings() {
        List<Object> processList = Arrays.asList("Text A", "Text B");
        List<Object> existingList = Arrays.asList("Text A", "Text B");
        List<Object> embeddingList = Arrays.asList(Arrays.asList(0.1, 0.2), Arrays.asList(0.3, 0.4));

        String fullEmbeddingKey = "embeddingField";

        Object result = textEmbeddingInferenceFilter.copyEmbedding(
            fullEmbeddingKey,
            processList,
            existingList,
            embeddingList,
            sourceAndMetadataMap,
            -1
        );

        assertNull(result);
        assertEquals(embeddingList, sourceAndMetadataMap.get(fullEmbeddingKey));
    }

    public void test_filterInferenceValuesInList_ListPartiallyChanged_ShouldNotCopyEmbeddings() {
        List<Object> processList = Arrays.asList("Text A", "New Text");
        List<Object> existingList = Arrays.asList("Text A", "Text B");
        List<Object> embeddingList = Arrays.asList(Arrays.asList(0.1, 0.2), Arrays.asList(0.3, 0.4));

        String fullEmbeddingKey = "embeddingField";

        Object result = textEmbeddingInferenceFilter.copyEmbedding(
            fullEmbeddingKey,
            processList,
            existingList,
            embeddingList,
            sourceAndMetadataMap,
            -1
        );

        assertEquals(processList, result);
        assertNull(sourceAndMetadataMap.get(fullEmbeddingKey));
    }

    public void test_filterInferenceValuesInList_NoMatchingField_ShouldNotCopyEmbeddings() {
        List<Object> processList = Arrays.asList("Text A", "Text B");
        String fullEmbeddingKey = "embeddingField";

        Object result = textEmbeddingInferenceFilter.copyEmbedding(
            fullEmbeddingKey,
            processList,
            Collections.emptyList(),
            Collections.emptyList(),
            sourceAndMetadataMap,
            -1
        );

        assertEquals(processList, result);
        assertNull(sourceAndMetadataMap.get(fullEmbeddingKey));
    }

    public void test_filter_nestedMapValue_Unchanged_ShouldCopyEmbeddings() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("embeddingField", "Hello World");
        Map<String, Object> processMap = new HashMap<>();
        processMap.put("outerField", nestedMap);

        Map<String, Object> existingNestedMap = new HashMap<>();
        existingNestedMap.put("textField", "Hello World");
        existingNestedMap.put("embeddingField", Arrays.asList(0.1, 0.2, 0.3));

        Map<String, Object> existingMap = new HashMap<>();
        existingMap.put("outerField", existingNestedMap);

        Map<String, Object> result = nestedTextEmbeddingInferenceFilter.filter(existingMap, sourceAndMetadataMap, processMap);

        assertNull(((Map) result.get("outerField")).get("embeddingField"));
        assertEquals(Arrays.asList(0.1, 0.2, 0.3), ((Map) sourceAndMetadataMap.get("outerField")).get("embeddingField"));
    }

    public void testFilter_nestedMapValue_PartiallyChanged_ShouldNotCopyEmbeddings() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("embeddingField", "New Text");
        Map<String, Object> processMap = new HashMap<>();
        processMap.put("outerField", nestedMap);

        Map<String, Object> existingNestedMap = new HashMap<>();
        existingNestedMap.put("textField", "Old Text");
        existingNestedMap.put("embeddingField", Arrays.asList(0.1, 0.2, 0.3));
        Map<String, Object> existingMap = new HashMap<>();
        existingMap.put("outerField", existingNestedMap);

        Map<String, Object> result = nestedTextEmbeddingInferenceFilter.filter(existingMap, sourceAndMetadataMap, processMap);

        assertFalse(result.isEmpty());
        assertEquals("New Text", ((Map<String, Object>) result.get("outerField")).get("embeddingField"));
        assertNull(sourceAndMetadataMap.get("outerField"));
    }

    public void test_filter_nestedListValue_Unchanged_ShouldCopyEmbeddings() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("embeddingField", Arrays.asList("Hello World", "Bye World"));
        Map<String, Object> processMap = new HashMap<>();
        processMap.put("outerField", nestedMap);

        Map<String, Object> existingNestedMap = new HashMap<>();
        existingNestedMap.put("textField", Arrays.asList("Hello World", "Bye World"));
        existingNestedMap.put("embeddingField", Arrays.asList(Arrays.asList(0.1, 0.2, 0.3), Arrays.asList(0.4, 0.5, 0.6)));

        Map<String, Object> existingMap = new HashMap<>();
        existingMap.put("outerField", existingNestedMap);

        Map<String, Object> result = nestedTextEmbeddingInferenceFilter.filter(existingMap, sourceAndMetadataMap, processMap);

        assertNull(((Map) result.get("outerField")).get("embeddingField"));
        assertEquals(Arrays.asList(0.1, 0.2, 0.3), ((List) ((Map) sourceAndMetadataMap.get("outerField")).get("embeddingField")).get(0));
        assertEquals(Arrays.asList(0.4, 0.5, 0.6), ((List) ((Map) sourceAndMetadataMap.get("outerField")).get("embeddingField")).get(1));
    }

    public void test_filter_nestedListValue_PartiallyChanged_ShouldNotCopyEmbeddings() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("embeddingField", Arrays.asList("Hello World", "Bye World"));
        Map<String, Object> processMap = new HashMap<>();
        processMap.put("outerField", nestedMap);

        Map<String, Object> existingNestedMap = new HashMap<>();
        existingNestedMap.put("textField", Arrays.asList("Hello World", "Goodbye World"));
        existingNestedMap.put("embeddingField", Arrays.asList(Arrays.asList(0.1, 0.2, 0.3), Arrays.asList(0.4, 0.5, 0.6)));

        Map<String, Object> existingMap = new HashMap<>();
        existingMap.put("outerField", existingNestedMap);

        Map<String, Object> result = nestedTextEmbeddingInferenceFilter.filter(existingMap, sourceAndMetadataMap, processMap);

        assertEquals(2, ((List) ((Map) result.get("outerField")).get("embeddingField")).size());
        assertNull(sourceAndMetadataMap.get("outerField"));
    }
}
