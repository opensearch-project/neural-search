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

import lombok.Getter;

/**
 * enum for distinguishing various reranking methods
 */
public enum RerankType {

    CROSS_ENCODER("cross-encoder");

    @Getter
    private final String label;

    private RerankType(String label) {
        this.label = label;
    }

    /**
     * Construct a RerankType from the label
     * @param label label of a RerankType
     * @return RerankType represented by the label
     */
    public static RerankType from(String label) {
        try {
            return RerankType.valueOf(label);
        } catch (Exception e) {
            throw new IllegalArgumentException("Wrong rerank type name: " + label);
        }
    }
}
