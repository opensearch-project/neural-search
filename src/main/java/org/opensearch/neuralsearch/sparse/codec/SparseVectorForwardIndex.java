/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

public interface SparseVectorForwardIndex extends ForwardIndex {
    @Override
    SparseVectorForwardIndexReader getForwardIndexReader();  // covariant return type

    @Override
    SparseVectorForwardIndexWriter getForwardIndexWriter();  // covariant return type

    interface SparseVectorForwardIndexReader extends ForwardIndex.ForwardIndexReader {
        SparseVector readSparseVector(int docId);
    }

    interface SparseVectorForwardIndexWriter extends ForwardIndex.ForwardIndexWriter {
        void write(int docId, SparseVector vector);
    }

    static SparseVectorForwardIndex getOrCreate(IndexKey key) {
        return InMemorySparseVectorForwardIndex.getOrCreate(key);
    }

    static void removeIndex(IndexKey key) {
        InMemorySparseVectorForwardIndex.removeIndex(key);
    }

    @AllArgsConstructor
    public static class IndexKey {
        private SegmentInfo segmentInfo;
        private FieldInfo fieldInfo;

        @Override
        public int hashCode() {
            return segmentInfo.hashCode() + fieldInfo.name.hashCode();
        }
    }
}
