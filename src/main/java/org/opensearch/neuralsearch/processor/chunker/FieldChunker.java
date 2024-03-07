/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;

public interface FieldChunker {
    void validateParameters(Map<String, Object> parameters);

    List<String> chunk(String content, Map<String, Object> parameters);
}
