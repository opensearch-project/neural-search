/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.constants;

import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.neuralsearch.processor.InferenceRequest;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestCommonConstants {
    public static final String MODEL_ID = "modeId";
    public static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    public static final Float[] PREDICT_VECTOR_ARRAY = new Float[] { 2.0f, 3.0f };
    public static final List<String> SENTENCES_LIST = List.of("TEXT");
    public static final Map<String, String> SENTENCES_MAP = Map.of("inputText", "Text query", "inputImage", "base641234567890");
    public static final InferenceRequest INFERENCE_REQUEST = InferenceRequest.builder()
        .modelId(MODEL_ID)
        .inputTexts(SENTENCES_LIST)
        .inputObjects(SENTENCES_MAP)
        .queryText("is it sunny")
        .targetResponseFilters(TARGET_RESPONSE_FILTERS)
        .build();
}
