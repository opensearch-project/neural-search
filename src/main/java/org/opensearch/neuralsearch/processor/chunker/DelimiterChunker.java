/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.List;
import java.util.Map;

public class DelimiterChunker implements IFieldChunker {

    public DelimiterChunker() {}

    @Override
    public void validateParameters(Map<String, Object> parameters) {
        throw new UnsupportedOperationException("delimiter chunker has not been implemented yet");
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        throw new UnsupportedOperationException("delimiter chunker has not been implemented yet");
    }
}
