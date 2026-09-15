/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulates metadata detected while streaming a segment's doc values, such as doc IDs and the
 * auto-detected dimension (max token ID + 1). Both native writers need it, and neither knows the
 * dimension until the last document has been read.
 */
@Getter
class StreamedVectorsMetadata {
    private final List<Integer> docIds = new ArrayList<>();
    private int maxTokenId = 0;

    void updateMaxTokenId(List<Integer> tokens) {
        if (!tokens.isEmpty()) {
            // Doc-value tokens are stored in parser order, not sorted, so the last
            // element is not the largest. Undersizing the dimension here makes
            // nsparse reject the segment with "term_id out of range".
            maxTokenId = Math.max(maxTokenId, Collections.max(tokens));
        }
    }

    int getDimension() {
        return maxTokenId + 1;
    }
}
