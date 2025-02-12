/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.constants;

import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestCommonConstants {
    public static final String MODEL_ID = "modeId";
    public static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    public static final Float[] PREDICT_VECTOR_ARRAY = new Float[] { 2.0f, 3.0f };
    public static final List<String> SENTENCES_LIST = List.of("it is sunny today", "roses are red");
    public static final Map<String, String> SENTENCES_MAP = Map.of("inputText", "Text query", "inputImage", "base641234567890");

    public static final String QUERY_TEST = "is it sunny";

    public static final TextInferenceRequest TEXT_INFERENCE_REQUEST = TextInferenceRequest.builder()
        .modelId(MODEL_ID)
        .inputTexts(SENTENCES_LIST)
        .build();

    public static final MapInferenceRequest MAP_INFERENCE_REQUEST = MapInferenceRequest.builder()
        .modelId(MODEL_ID)
        .inputObjects(SENTENCES_MAP)
        .build();

    public static final SimilarityInferenceRequest SIMILARITY_INFERENCE_REQUEST = SimilarityInferenceRequest.builder()
        .modelId(MODEL_ID)
        .inputTexts(SENTENCES_LIST)
        .queryText(QUERY_TEST)
        .build();
}
