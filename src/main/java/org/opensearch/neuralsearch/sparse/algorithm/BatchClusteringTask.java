/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsReader;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        this.terms = terms;
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
        try {
            for (BytesRef term : this.terms) {
                Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap = new HashMap<>();
                List<DocFreq> docFreqs = SparsePostingsReader.getMergedPostingForATerm(
                    this.mergeState,
                    term,
                    this.fieldInfo,
                    newToOldDocIdMap
                );
                PostingClustering postingClustering = new PostingClustering(
                    nPostings,
                    new RandomClustering(summaryPruneRatio, clusterRatio, (newDocId) -> {
                        Pair<Integer, InMemoryKey.IndexKey> oldDocId = newToOldDocIdMap.get(newDocId);
                        if (oldDocId != null) {
                            InMemorySparseVectorForwardIndex oldIndex = InMemorySparseVectorForwardIndex.get(oldDocId.getRight());
                            if (oldIndex != null) {
                                return oldIndex.getForwardIndexReader().readSparseVector(oldDocId.getLeft());
                            }
                        }
                        InMemorySparseVectorForwardIndex newIndex = InMemorySparseVectorForwardIndex.get(this.key);
                        if (newIndex != null) {
                            SparseVector vector = newIndex.getForwardIndexReader().readSparseVector(newDocId);
                            return vector;
                        }
                        return null;
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
}
