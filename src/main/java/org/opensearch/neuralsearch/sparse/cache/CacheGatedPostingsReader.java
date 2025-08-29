/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.codec.SparseTermsLuceneReader;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;

import java.io.IOException;
import java.util.Set;

/**
 * A cache-gated clustered posting reader that implements a two-tier read strategy for sparse term postings.
 * Cache misses are automatically populated from the underlying storage.
 */
public class CacheGatedPostingsReader implements ClusteredPostingReader {

    private final String fieldName;
    private final ClusteredPostingReader cacheReader;
    private final ClusteredPostingWriter cacheWriter;
    private final SparseTermsLuceneReader luceneReader;

    /**
     * Constructs a new cache-gated clustered posting reader.
     *
     * @param fieldName the field name for which to read postings
     * @param cacheReader the reader for accessing cached postings
     * @param cacheWriter the writer for populating the cache
     * @param luceneReader the reader for accessing postings from Lucene storage
     * @throws NullPointerException if any parameter is null
     */
    public CacheGatedPostingsReader(
        @NonNull String fieldName,
        @NonNull ClusteredPostingReader cacheReader,
        @NonNull ClusteredPostingWriter cacheWriter,
        @NonNull SparseTermsLuceneReader luceneReader
    ) {
        this.fieldName = fieldName;
        this.cacheReader = cacheReader;
        this.cacheWriter = cacheWriter;
        this.luceneReader = luceneReader;
    }

    /**
     * Reads a clustered posting given the specified term.
     *
     * Read Strategy:
     * 1. First attempts to read from the cache
     * 2. On cache miss, reads from Lucene storage
     * 3. Automatically populates the cache with the retrieved posting
     *
     * @param term the term for which to retrieve the clustered posting
     * @return the clustered posting associated with the term, or null if the posting does not exist
     * @throws IOException if an I/O error occurs while reading
     */
    @Override
    public PostingClusters read(BytesRef term) throws IOException {
        PostingClusters clusters = cacheReader.read(term);
        if (clusters != null) {
            return clusters;
        }

        clusters = luceneReader.read(fieldName, term);

        if (clusters != null) {
            cacheWriter.insert(term, clusters.getClusters());
        }
        return clusters;
    }

    // we return terms from lucene as cache may not have all data due to memory constraint
    @Override
    public Set<BytesRef> getTerms() {
        return luceneReader.getTerms(fieldName);
    }

    @Override
    public long size() {
        return luceneReader.getTerms(fieldName).size();
    }
}
