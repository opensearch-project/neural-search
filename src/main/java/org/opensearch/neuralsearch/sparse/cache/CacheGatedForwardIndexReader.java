/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;

/**
 * A cache-gated forward index reader that implements a two-tier read strategy for sparse vectors.
 * Cache misses are automatically populated from the underlying storage.
 */
public class CacheGatedForwardIndexReader implements SparseVectorReader {

    /**
     * A no-op implementation of SparseVectorReader that always returns null.
     */
    private static final SparseVectorReader NOOP_READER = docId -> null;

    /**
     * A no-op implementation of SparseVectorWriter that ignores all write operations.
     */
    private static final SparseVectorWriter NOOP_WRITER = (docId, vector) -> {};

    private final SparseVectorReader cacheReader;
    private final SparseVectorWriter cacheWriter;
    private final SparseVectorReader luceneReader;

    /**
     * Constructs a new cache-gated forward index reader.
     *
     * @param cacheReader the reader for accessing cached sparse vectors in cache
     * @param cacheWriter the writer for populating the cache
     * @param luceneReader the reader for accessing sparse vectors from Lucene storage
     */
    public CacheGatedForwardIndexReader(SparseVectorReader cacheReader, SparseVectorWriter cacheWriter, SparseVectorReader luceneReader) {
        this.cacheReader = cacheReader == null ? NOOP_READER : cacheReader;
        this.cacheWriter = cacheWriter == null ? NOOP_WRITER : cacheWriter;
        this.luceneReader = luceneReader == null ? NOOP_READER : luceneReader;
    }

    /**
     * Reads a sparse vector given the specified document ID.
     *
     * Read Strategy:
     * 1. First attempts to read from the cache
     * 2. On cache miss, reads from Lucene storage
     * 3. Automatically populates the cache with the retrieved vector
     *
     * @param docId the document ID for which to retrieve the sparse vector
     * @return the sparse vector associated with the document ID, or null if the vector does not exist
     * @throws IOException if an I/O error occurs while reading
     */
    @Override
    public SparseVector read(int docId) throws IOException {
        SparseVector vector = cacheReader.read(docId);
        if (vector != null) {
            return vector;
        }
        // synchronize luceneReader for thread safety
        synchronized (luceneReader) {
            vector = luceneReader.read(docId);
        }
        if (vector != null) {
            cacheWriter.insert(docId, vector);
        }
        return vector;
    }
}
