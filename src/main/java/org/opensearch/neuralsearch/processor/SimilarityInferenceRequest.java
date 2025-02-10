/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.NoArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * Implementation of InferenceRequest for similarity based text inference requests.
 *
 * @see TextInferenceRequest
 */
@SuperBuilder
@NoArgsConstructor
@Getter
@Setter
public class SimilarityInferenceRequest extends TextInferenceRequest {
    private String queryText;
}
