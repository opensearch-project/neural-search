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

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import lombok.Getter;

/**
 * enum for distinguishing various reranking methods
 */
public enum RerankType {

    TEXT_SIMILARITY("text_similarity");

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
        Optional<RerankType> typeMaybe = Arrays.stream(RerankType.values()).filter(rrt -> rrt.label.equals(label)).findFirst();
        if (typeMaybe.isPresent()) {
            return typeMaybe.get();
        } else {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Wrong rerank type name: %s", label));
        }
    }
}
