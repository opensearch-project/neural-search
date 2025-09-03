/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCacheItem;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Sparse terms implementation
 */
@Getter
public class SparseTerms extends Terms {
    private final CacheKey cacheKey;
    private final CacheGatedPostingsReader reader;

    public SparseTerms(CacheKey cacheKey, SparseTermsLuceneReader sparseTermsLuceneReader, String field) {
        this.cacheKey = cacheKey;
        ClusteredPostingCacheItem clusteredPostingCacheItem = ClusteredPostingCache.getInstance().getOrCreate(cacheKey);
        this.reader = new CacheGatedPostingsReader(
            field,
            clusteredPostingCacheItem.getReader(),
            clusteredPostingCacheItem.getWriter(),
            sparseTermsLuceneReader
        );
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new SparseTermsEnum();
    }

    @Override
    public long size() throws IOException {
        return this.reader.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        return 0;
    }

    @Override
    public long getSumDocFreq() throws IOException {
        return 0;
    }

    @Override
    public int getDocCount() throws IOException {
        return 0;
    }

    @Override
    public boolean hasFreqs() {
        return false;
    }

    @Override
    public boolean hasOffsets() {
        return false;
    }

    @Override
    public boolean hasPositions() {
        return false;
    }

    @Override
    public boolean hasPayloads() {
        return false;
    }

    class SparseTermsEnum extends BaseTermsEnum {
        private BytesRef currentTerm;
        // iterator now only used for next()
        private Iterator<BytesRef> termIterator;

        SparseTermsEnum() throws IOException {
            Set<BytesRef> terms = reader.getTerms();
            if (terms != null) {
                termIterator = terms.iterator();
            }
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            if (reader.read(text) == null) {
                return SeekStatus.NOT_FOUND;
            }
            currentTerm = text.clone();
            return SeekStatus.FOUND;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef term() throws IOException {
            return this.currentTerm;
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            if (currentTerm == null) {
                return null;
            }
            PostingClusters clusters = reader.read(currentTerm);
            if (clusters != null) {
                return new SparsePostingsEnum(clusters, cacheKey);
            }
            return null;
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() throws IOException {
            if (termIterator == null || !termIterator.hasNext()) {
                this.currentTerm = null;
                return null;
            }
            this.currentTerm = termIterator.next();
            return this.currentTerm;
        }
    }
}
