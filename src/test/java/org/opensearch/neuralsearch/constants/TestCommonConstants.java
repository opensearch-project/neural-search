/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.constants;

import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestCommonConstants {
    public static final String MODEL_ID = "modeId";
    public static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    public static final Float[] PREDICT_VECTOR_ARRAY = new Float[] { 2.0f, 3.0f };
    public static final List<String> SENTENCES_LIST = List.of("TEXT");
}
