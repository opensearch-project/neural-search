/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.Data;

@Data
public class SparseQueryResult {
    private final int id;
    private final float score;
}
