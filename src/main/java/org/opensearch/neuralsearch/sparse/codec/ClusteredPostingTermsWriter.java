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
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.io.IOException;

/**
 * ClusteredPostingTermsWriter is used to write postings for each segment
 */
public class ClusteredPostingTermsWriter {
    private final PostingsWriterBase postingsWriter;
    private final FixedBitSet docsSeen;

    public ClusteredPostingTermsWriter(SegmentWriteState state, FieldInfo fieldInfo) {
        super();
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(state.segmentInfo, fieldInfo);
        SparseVectorForwardIndex index = SparseVectorForwardIndex.getOrCreate(key);
        int beta = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.BETA_FIELD));
        int lambda = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.LAMBDA_FIELD));
        float alpha = Float.parseFloat(fieldInfo.attributes().get(SparseMethodContext.ALPHA_FIELD));
        int clusterUntilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.CLUSTER_UNTIL_FIELD));
        this.postingsWriter = new InMemoryClusteredPosting.InMemoryClusteredPostingWriter(
            key,
            fieldInfo,
            new PostingClustering(lambda, new KMeansPlusPlus(alpha, clusterUntilDocCountReach > 0 ? 1 : beta, (docId) -> index.getForwardIndexReader().readSparseVector(docId)))
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
