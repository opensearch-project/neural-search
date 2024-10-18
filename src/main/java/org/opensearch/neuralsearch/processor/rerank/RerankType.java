/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

    ML_OPENSEARCH("ml_opensearch"),
    BY_FIELD("by_field");

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
    public static RerankType from(final String label) {
        RerankType ans = LABEL_MAP.get(label);
        if (ans == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Wrong rerank type name: %s", label));
        }
        return ans;
    }

    public static Map<String, RerankType> labelMap() {
        return LABEL_MAP;
    }
}
