/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Builder
@AllArgsConstructor
@Getter
@Setter
/**
 * POJO class to hold request parameters to call ml commons client accessor.
 */
public class InferenceRequest {
    @NonNull
    private String modelId; // required
    private List<String> inputTexts; // on which inference needs to happen
    private Map<String, String> inputObjects;
    private List<String> targetResponseFilters;
    private String queryText;
}
