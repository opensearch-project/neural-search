/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.util.pruning.PruneType;
import org.opensearch.test.OpenSearchTestCase;

public class TokenWeightUtilTests extends OpenSearchTestCase {
    private static final Map<String, Float> MOCK_DATA = Map.of("hello", 1.f, "world", 2.f);

    public void testFetchListOfTokenWeightMap_singleObject() {
        /*
          [{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(MOCK_DATA)));
        assertEquals(TokenWeightUtil.fetchListOfTokenWeightMap(inputData), List.of(MOCK_DATA));
    }

    public void testFetchListOfTokenWeightMap_multipleObjectsInOneResponse() {
        /*
          [{
              "response": [
                {"hello": 1.0, "world": 2.0},
                {"hello": 1.0, "world": 2.0}
              ]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(MOCK_DATA, MOCK_DATA)));
        assertEquals(TokenWeightUtil.fetchListOfTokenWeightMap(inputData), List.of(MOCK_DATA, MOCK_DATA));
    }

    public void testFetchListOfTokenWeightMap_multipleObjectsInMultipleResponse() {
        /*
          [{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          },{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(MOCK_DATA)), Map.of("response", List.of(MOCK_DATA)));
        assertEquals(TokenWeightUtil.fetchListOfTokenWeightMap(inputData), List.of(MOCK_DATA, MOCK_DATA));
    }

    public void testFetchListOfTokenWeightMap_whenResponseValueNotList_thenFail() {
        /*
          [{
              "response": {"hello": 1.0, "world": 2.0}
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", MOCK_DATA));
        expectThrows(IllegalArgumentException.class, () -> TokenWeightUtil.fetchListOfTokenWeightMap(inputData));
    }

    public void testFetchListOfTokenWeightMap_whenNotUseResponseKey_thenFail() {
        /*
          [{
              "some_key": [{"hello": 1.0, "world": 2.0}]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("some_key", List.of(MOCK_DATA)));
        expectThrows(IllegalArgumentException.class, () -> TokenWeightUtil.fetchListOfTokenWeightMap(inputData));
    }

    public void testFetchListOfTokenWeightMap_whenInputObjectIsNotMap_thenFail() {
        /*
          [{
              "response": [[{"hello": 1.0, "world": 2.0}]]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(List.of(MOCK_DATA))));
        expectThrows(IllegalArgumentException.class, () -> TokenWeightUtil.fetchListOfTokenWeightMap(inputData));
    }

    public void testFetchListOfTokenWeightMap_whenInputTokenMapWithNonStringKeys_thenFail() {
        /*
          [{
              "response": [{"hello": 1.0, 2.3: 2.0}]
          }]
        */
        Map<?, Float> mockData = Map.of("hello", 1.f, 2.3f, 2.f);
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(mockData)));
        expectThrows(IllegalArgumentException.class, () -> TokenWeightUtil.fetchListOfTokenWeightMap(inputData));
    }

    public void testFetchListOfTokenWeightMap_whenInputTokenMapWithNonFloatValues_thenFail() {
        /*
          [{
              "response": [{"hello": 1.0, "world": "world"}]
          }]
        */
        Map<String, ?> mockData = Map.of("hello", 1.f, "world", "world");
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(mockData)));
        expectThrows(IllegalArgumentException.class, () -> TokenWeightUtil.fetchListOfTokenWeightMap(inputData));
    }

    public void testFetchListOfTokenWeightMap_invokeWithPrune() {
        /*
          [{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(MOCK_DATA)));
        assertEquals(TokenWeightUtil.fetchListOfTokenWeightMap(inputData, PruneType.MAX_RATIO, 0.8f), List.of(Map.of("world", 2f)));
    }

    public void testFetchListOfTokenWeightMap_invokeWithPrune_MultipleObjectsInMultipleResponse() {

        /*
          [{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          },{
              "response": [
                {"hello": 1.0, "world": 2.0}
              ]
          }]
        */
        List<Map<String, ?>> inputData = List.of(Map.of("response", List.of(MOCK_DATA)), Map.of("response", List.of(MOCK_DATA)));
        assertEquals(
            TokenWeightUtil.fetchListOfTokenWeightMap(inputData, PruneType.TOP_K, 1f),
            List.of(Map.of("world", 2f), Map.of("world", 2f))
        );
    }
}
