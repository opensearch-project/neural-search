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

public class CacheGatedPostingsReader implements ClusteredPostingReader {
    private final String fieldName;
    private final ClusteredPostingReader cacheReader;
    private final ClusteredPostingWriter cacheWriter;
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseTermsLuceneReader luceneReader;

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

    @Override
    public PostingClusters read(BytesRef term) throws IOException {
        PostingClusters clusters = cacheReader.read(term);
        if (clusters != null) {
            return clusters;
        }
        // if cluster does not exist in cache, read from lucene and populate it to cache
        synchronized (luceneReader) {
            clusters = luceneReader.read(fieldName, term);
        }

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
