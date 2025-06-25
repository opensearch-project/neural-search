/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.io.IOException;

@FunctionalInterface
public interface SparseVectorReader {
    SparseVector read(int docId) throws IOException;
}
