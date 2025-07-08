/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public interface SparseVectorWriter {
    void write(int docId, SparseVector vector) throws IOException;

    void write(int docId, BytesRef data) throws IOException;
}
