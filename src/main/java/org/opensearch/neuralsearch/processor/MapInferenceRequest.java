/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * Implementation of InferenceRequest for inputObjects based inference requests.
 * Use this class when the input data consists of key-value pairs.
 *
 * @see InferenceRequest
 */
@SuperBuilder
@NoArgsConstructor
@Getter
@Setter
public class MapInferenceRequest extends InferenceRequest {
    private Map<String, String> inputObjects;
}
