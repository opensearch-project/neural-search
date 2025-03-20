/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.highlight;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.opensearch.neuralsearch.processor.InferenceRequest;

/**
 * Implementation of InferenceRequest for sentence highlighting inference requests.
 * This class handles the question and context parameters needed for highlighting.
 *
 * @see InferenceRequest
 */
@SuperBuilder
@NoArgsConstructor
@Getter
@Setter
public class SentenceHighlightingRequest extends InferenceRequest {
    /**
     * The question to be answered from the context
     */
    private String question;

    /**
     * The context text in which to find the answer
     */
    private String context;
}
