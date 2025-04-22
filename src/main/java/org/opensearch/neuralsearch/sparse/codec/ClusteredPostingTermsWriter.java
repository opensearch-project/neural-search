/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.neuralsearch.sparse.algorithm.KMeansPlusPlus;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;

/**
 * ClusteredPostingTermsWriter is used to write postings for each segment
 */
public class ClusteredPostingTermsWriter {
    private final PostingsWriterBase postingsWriter;
    private final FixedBitSet docsSeen;
    private static final int DEFAULT_LAMBDA = 20;
    private static final int DEFAULT_BETA = 2;

    public ClusteredPostingTermsWriter(SegmentWriteState state, FieldInfo fieldInfo) {
        super();
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(state.segmentInfo, fieldInfo);
        SparseVectorForwardIndex index = SparseVectorForwardIndex.getOrCreate(key);
        this.postingsWriter = new InMemoryClusteredPosting.InMemoryClusteredPostingWriter(
            key,
            fieldInfo,
            new PostingClustering(DEFAULT_LAMBDA, new KMeansPlusPlus(DEFAULT_BETA, index.getForwardIndexReader()))
        );
        this.docsSeen = new FixedBitSet(state.segmentInfo.maxDoc());
    }

    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
        if (this.postingsWriter instanceof InMemoryClusteredPosting.InMemoryClusteredPostingWriter) {
            InMemoryClusteredPosting.InMemoryClusteredPostingWriter writer =
                (InMemoryClusteredPosting.InMemoryClusteredPostingWriter) this.postingsWriter;
            writer.writeInMemoryTerm(text, termsEnum, this.docsSeen, norms);
        } else {
            throw new RuntimeException("not support");
        }
    }
}
