/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.MergeHelper;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Task for clustering postings in batches during index merging.
 * Processes multiple terms and generates clustered postings for sparse vector optimization.
 */
@Log4j2
public class BatchClusteringTask implements Supplier<List<Pair<BytesRef, PostingClusters>>> {
    @Getter
    private final List<BytesRef> terms;
    private final CacheKey key;
    private final float summaryPruneRatio;
    private final float clusterRatio;
    private final int nPostings;
    private final MergeStateFacade mergeStateFacade;
    private final FieldInfo fieldInfo;
    private final MergeHelper mergeHelper;

    /**
     * Creates a batch clustering task.
     *
     * @param terms list of terms to cluster
     * @param key cache key for storing results
     * @param summaryPruneRatio ratio for pruning summary vectors
     * @param clusterRatio ratio for clustering algorithm
     * @param nPostings number of postings to process
     * @param mergeStateFacade merge state containing segment information
     * @param fieldInfo field information for the sparse vector field
     */
    public BatchClusteringTask(
        List<BytesRef> terms,
        CacheKey key,
        float summaryPruneRatio,
        float clusterRatio,
        int nPostings,
        @NonNull MergeStateFacade mergeStateFacade,
        FieldInfo fieldInfo,
        MergeHelper mergeHelper
    ) {
        this.terms = terms.stream().map(BytesRef::deepCopyOf).toList();
        this.key = key;
        this.summaryPruneRatio = summaryPruneRatio;
        this.clusterRatio = clusterRatio;
        this.nPostings = nPostings;
        this.mergeStateFacade = mergeStateFacade;
        this.fieldInfo = fieldInfo;
        this.mergeHelper = mergeHelper;
    }

    /**
     * Executes the clustering task and returns clustered postings for all terms.
     *
     * @return list of term-cluster pairs
     */
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
                List<DocWeight> docWeights = mergeHelper.getMergedPostingForATerm(
                    this.mergeStateFacade,
                    term,
                    this.fieldInfo,
                    newIdToFieldProducerIndex,
                    newIdToOldId
                );
                SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(
                    nPostings,
                    new RandomClusteringAlgorithm(summaryPruneRatio, clusterRatio, (newDocId) -> {
                        int oldId = newIdToOldId[newDocId];
                        int segmentIndex = newIdToFieldProducerIndex[newDocId];
                        BinaryDocValues binaryDocValues = mergeStateFacade.getDocValuesProducers()[segmentIndex].getBinary(fieldInfo);
                        SparseVectorReader reader = getCacheGatedForwardIndexReader(binaryDocValues);
                        return reader.read(oldId);
                    })
                );
                List<DocumentCluster> clusters = seismicPostingClusterer.cluster(docWeights);
                postingClusters.add(Pair.of(term, new PostingClusters(clusters)));
                ClusteredPostingWriter writer = ClusteredPostingCache.getInstance().getOrCreate(key).getWriter();
                writer.insert(term, clusters);
            }
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        return postingClusters;
    }

    private int getTotalDocs() {
        int maxDocs = 0;
        for (int i = 0; i < this.mergeStateFacade.getMaxDocs().length; ++i) {
            maxDocs += this.mergeStateFacade.getMaxDocs()[i];
        }
        return maxDocs;
    }

    /**
     * Creates a createSparseVectorReader for vector access.
     *
     * @param binaryDocValues binaryDocValues The binary doc values containing sparse vector data
     * @return A SparseVectorReader instance
     */
    private SparseVectorReader getCacheGatedForwardIndexReader(BinaryDocValues binaryDocValues) {
        if (binaryDocValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValues) {
            SegmentInfo segmentInfo = sparseBinaryDocValues.getSegmentInfo();
            CacheKey cacheKey = new CacheKey(segmentInfo, fieldInfo);
            ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
            if (index == null) {
                return new CacheGatedForwardIndexReader(null, null, sparseBinaryDocValues);
            }
            return new CacheGatedForwardIndexReader(index.getReader(), index.getWriter(), sparseBinaryDocValues);
        } else {
            return SparseVectorReader.NOOP_READER;
        }
    }
}
