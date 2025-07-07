/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class TextEmbeddingInfo {
    private Integer embeddingDimension;
    private Map<String, Object> additionalConfig;
}
