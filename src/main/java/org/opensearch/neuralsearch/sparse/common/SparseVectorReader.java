/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

@FunctionalInterface
public interface SparseVectorReader {
    SparseVector read(int docId);
}
