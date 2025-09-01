/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Context for sparse query
 */
@Data
@Builder
public class SparseQueryContext {
    private final List<String> tokens;
    private final float heapFactor;
    private final int k;
}
