/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsReader;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class BatchClusteringTask implements Supplier<List<Pair<BytesRef, PostingClusters>>> {
    private final List<BytesRef> terms;
    private final InMemoryKey.IndexKey key;
    private final float summaryPruneRatio;
    private final float clusterRatio;
    private final int nPostings;
    private final MergeState mergeState;
    private final FieldInfo fieldInfo;

    public BatchClusteringTask(
        List<BytesRef> terms,
        InMemoryKey.IndexKey key,
        float summaryPruneRatio,
        float clusterRatio,
        int nPostings,
        MergeState mergeState,
        FieldInfo fieldInfo
    ) {
        this.terms = terms.stream().map(BytesRef::deepCopyOf).toList();
        this.key = key;
        this.summaryPruneRatio = summaryPruneRatio;
        this.clusterRatio = clusterRatio;
        this.nPostings = nPostings;
        this.mergeState = mergeState;
        this.fieldInfo = fieldInfo;
    }

    @Override
    public List<Pair<BytesRef, PostingClusters>> get() {
        List<Pair<BytesRef, PostingClusters>> postingClusters = new ArrayList<>();
        int maxDocs = getTotalDocs();
        if (maxDocs == 0) {
            return postingClusters;
        }
        try {
            for (BytesRef term : this.terms) {
                int[] newIdToFieldProducerIndex = new int[maxDocs];
                int[] newIdToOldId = new int[maxDocs];
                List<DocFreq> docFreqs = SparsePostingsReader.getMergedPostingForATerm(
                    this.mergeState,
                    term,
                    this.fieldInfo,
                    newIdToFieldProducerIndex,
                    newIdToOldId
                );
                PostingClustering postingClustering = new PostingClustering(
                    nPostings,
                    new RandomClustering(summaryPruneRatio, clusterRatio, (newDocId) -> {
                        int oldId = newIdToOldId[newDocId];
                        int segmentIndex = newIdToFieldProducerIndex[newDocId];
                        BinaryDocValues binaryDocValues = mergeState.docValuesProducers[segmentIndex].getBinary(fieldInfo);
                        if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValues)) {
                            return null;
                        }
                        SegmentInfo oldSegment = sparseBinaryDocValues.getSegmentInfo();
                        SparseVector vector = readFromCacheOfOldSegment(oldSegment, oldId);
                        if (vector != null) {
                            return vector;
                        }
                        vector = readFromCacheOfMergedSegment(newDocId);
                        if (vector != null) {
                            return vector;
                        }
                        // read from old segment's lucene
                        return sparseBinaryDocValues.read(oldId);
                    })
                );
                List<DocumentCluster> clusters = postingClustering.cluster(docFreqs);
                postingClusters.add(Pair.of(term, new PostingClusters(clusters)));
                InMemoryClusteredPosting.InMemoryClusteredPostingWriter.writePostingClusters(key, term, clusters);
            }
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        return postingClusters;
    }

    private SparseVector readFromCacheOfMergedSegment(int newDocId) throws IOException {
        InMemorySparseVectorForwardIndex newIndex = InMemorySparseVectorForwardIndex.get(this.key);
        return newIndex != null ? newIndex.getReader().read(newDocId) : null;
    }

    private SparseVector readFromCacheOfOldSegment(SegmentInfo oldSegment, int oldId) throws IOException {
        InMemoryKey.IndexKey oldKey = new InMemoryKey.IndexKey(oldSegment, fieldInfo);
        InMemorySparseVectorForwardIndex oldIndex = InMemorySparseVectorForwardIndex.get(oldKey);
        if (oldIndex != null) {
            return oldIndex.getReader().read(oldId);
        }
        return null;
    }

    private int getTotalDocs() {
        int maxDocs = 0;
        for (int i = 0; i < this.mergeState.maxDocs.length; ++i) {
            maxDocs += this.mergeState.maxDocs[i];
        }
        return maxDocs;
    }
}
