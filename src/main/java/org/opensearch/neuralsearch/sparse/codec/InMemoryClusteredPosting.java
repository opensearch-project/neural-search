/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class manages the in-memory postings for sparse vectors. It provides methods to write and read postings from memory.
 * It is used by the SparsePostingsConsumer and SparsePostingsReader classes.
 */
@Log4j2
public class InMemoryClusteredPosting implements ClusteredPosting, Accountable {

    private static final Map<InMemoryKey.IndexKey, InMemoryClusteredPosting> postingsMap = new ConcurrentHashMap<>();

    public static long memUsage() {
        long mem = RamUsageEstimator.shallowSizeOf(postingsMap);
        for (Map.Entry<InMemoryKey.IndexKey, InMemoryClusteredPosting> entry : postingsMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(entry.getKey());
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }

    public static synchronized InMemoryClusteredPosting getOrCreate(InMemoryKey.IndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return postingsMap.computeIfAbsent(key, k -> new InMemoryClusteredPosting());
    }

    public static InMemoryClusteredPosting get(InMemoryKey.IndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return postingsMap.get(key);
    }

    public static void clearIndex(InMemoryKey.IndexKey key) {
        postingsMap.remove(key);
    }

    private final Map<BytesRef, PostingClusters> clusteredPostings = new ConcurrentHashMap<>();
    private final AtomicLong usedRamBytes = new AtomicLong(RamUsageEstimator.shallowSizeOf(clusteredPostings));
    private final ClusteredPostingReader reader = new InMemoryClusteredPostingReader();
    private final ClusteredPostingWriter writer = new InMemoryClusteredPostingWriter();

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    @Override
    public ClusteredPostingReader getReader() {
        return reader;
    }

    @Override
    public ClusteredPostingWriter getWriter() {
        return writer;
    }

    private class InMemoryClusteredPostingReader implements ClusteredPostingReader {
        @Override
        public PostingClusters read(BytesRef term) {
            return clusteredPostings.get(term);
        }

        @Override
        public Set<BytesRef> getTerms() {
            return Collections.unmodifiableSet(clusteredPostings.keySet());
        }

        @Override
        public long size() {
            return clusteredPostings.size();
        }
    }

    private class InMemoryClusteredPostingWriter implements ClusteredPostingWriter {
        public synchronized void write(BytesRef term, List<DocumentCluster> clusters) {
            if (clusters == null || clusters.isEmpty()) {
                return;
            }

            BytesRef clonedTerm = term.clone();

            PostingClusters oldClusters = clusteredPostings.get(clonedTerm);
            long oldClustersSize = (oldClusters != null) ? oldClusters.ramBytesUsed() : 0;

            PostingClusters postingClusters = new PostingClusters(clusters);
            long newClustersSize = postingClusters.ramBytesUsed();
            long termSize = RamUsageEstimator.shallowSizeOf(clonedTerm) + (clonedTerm.bytes != null ? clonedTerm.bytes.length : 0);

            // Update the clusters
            clusteredPostings.put(clonedTerm, postingClusters);

            // Update memory usage tracking
            if (oldClusters == null) {
                // If adding new term, account for term size + clusters size
                usedRamBytes.addAndGet(termSize + newClustersSize);
            } else {
                // If updating existing term, just account for difference in clusters size
                usedRamBytes.addAndGet(newClustersSize - oldClustersSize);
            }
        }
    }
}
