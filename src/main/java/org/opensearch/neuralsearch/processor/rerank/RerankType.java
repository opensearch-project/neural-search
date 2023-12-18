/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import lombok.Getter;

/**
 * enum for distinguishing various reranking methods
 */
public enum RerankType {

    ML_OPENSEARCH("ml_opensearch");

    @Getter
    private final String label;

    private RerankType(String label) {
        this.label = label;
    }

    private static final Map<String, RerankType> LABEL_MAP;
    static {
        Map<String, RerankType> labelMap = new HashMap<>();
        for (RerankType type : RerankType.values()) {
            labelMap.put(type.getLabel(), type);
        }
        LABEL_MAP = Collections.unmodifiableMap(labelMap);
    }

    /**
     * Construct a RerankType from the label
     * @param label label of a RerankType
     * @return RerankType represented by the label
     */
    public static RerankType from(String label) {
        RerankType ans = LABEL_MAP.get(label);
        if (ans == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Wrong rerank type name: %s", label));
        } else {
            return ans;
        }
    }

    public static Map<String, RerankType> labelMap() {
        return LABEL_MAP;
    }
}
