/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * An interface for forward index. It provides a reader and writer to read and write document.
 */
public interface ForwardIndex {
    ForwardIndexReader getForwardIndexReader();

    ForwardIndexWriter getForwardIndexWriter();

    interface ForwardIndexReader {
        BytesRef read(int docId);
    }

    interface ForwardIndexWriter {
        void write(int docId, BytesRef doc) throws IOException;

        void close();
    }
}
