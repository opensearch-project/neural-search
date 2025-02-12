/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * Implementation of InferenceRequest for inputTexts based inference requests.
 * Use this class when the input data consists of list of strings.
 *
 * @see InferenceRequest
 */
@SuperBuilder
@NoArgsConstructor
@Getter
@Setter
public class TextInferenceRequest extends InferenceRequest {
    private List<String> inputTexts; // on which inference needs to happen
}
