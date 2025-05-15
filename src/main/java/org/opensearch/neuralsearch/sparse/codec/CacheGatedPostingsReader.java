/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Set;

public class CacheGatedPostingsReader {
    private final String field;
    private final InMemoryClusteredPosting.InMemoryClusteredPostingReader inMemoryReader;
    private final InMemoryKey.IndexKey indexKey;
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseTermsLuceneReader luceneReader;

    public CacheGatedPostingsReader(
        String field,
        InMemoryClusteredPosting.InMemoryClusteredPostingReader reader,
        SparseTermsLuceneReader luceneReader,
        InMemoryKey.IndexKey indexKey
    ) {
        this.field = field;
        this.inMemoryReader = reader;
        this.luceneReader = luceneReader;
        this.indexKey = indexKey;
    }

    // we return terms from lucene as cache may not have all data due to memory constraint
    public Set<BytesRef> terms() throws IOException {
        return luceneReader.getTerms(field);
    }

    public long size() {
        return luceneReader.getTerms(field).size();
    }

    public PostingClusters read(BytesRef term) throws IOException {
        PostingClusters clusters = inMemoryReader.read(term);
        // if cluster does not exist in cache, read from lucene and populate it to cache
        if (clusters == null) {
            clusters = luceneReader.read(field, term);
            if (clusters != null) {
                InMemoryClusteredPosting.InMemoryClusteredPostingWriter.writePostingClusters(indexKey, term, clusters.getClusters());
            }
        }
        return clusters;
    }
}
