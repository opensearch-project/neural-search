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
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Sparse terms implementation
 */
@Getter
public class SparseTerms extends Terms {

    private final InMemoryClusteredPosting.InMemoryClusteredPostingReader reader;
    private final InMemoryKey.IndexKey indexKey;

    public SparseTerms(InMemoryClusteredPosting.InMemoryClusteredPostingReader reader, InMemoryKey.IndexKey indexKey) {
        this.reader = reader;
        this.indexKey = indexKey;
    }

    @Override
    public TermsEnum iterator() throws IOException {
        SparseTermsEnum sparseTermsEnum = new SparseTermsEnum();
        return sparseTermsEnum;
    }

    @Override
    public long size() throws IOException {
        return 0;
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
        private final Set<BytesRef> terms;
        // iterator now only used for next()
        private Iterator<BytesRef> termIterator;

        SparseTermsEnum() {
            terms = reader.getTerms();
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
                return new SparsePostingsEnum(reader.read(currentTerm), indexKey);
            }
            return null;
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() throws IOException {
            if (!termIterator.hasNext()) {
                this.currentTerm = null;
                return null;
            }
            this.currentTerm = termIterator.next();
            return this.currentTerm;
        }
    }
}
