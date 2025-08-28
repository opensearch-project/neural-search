/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPosting;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This class manages the cache postings for sparse vectors. It provides methods to write and read postings from cache.
 * It is used by the SparsePostingsConsumer and SparsePostingsReader classes.
 */
@Log4j2
public class ClusteredPostingCacheItem implements ClusteredPosting, Accountable {

    private static final String CIRCUIT_BREAKER_LABEL = "Cache Clustered Posting";
    private final CacheKey cacheKey;
    private final Map<BytesRef, PostingClusters> clusteredPostings = new ConcurrentHashMap<>();
    private final AtomicLong usedRamBytes = new AtomicLong(RamUsageEstimator.shallowSizeOf(clusteredPostings));
    @Getter
    private final ClusteredPostingReader reader = new CacheClusteredPostingReader();
    @Getter
    private final CacheableClusteredPostingWriter writer = new CacheClusteredPostingWriter();

    /**
     * Returns the writer instance.
     * @param circuitBreakerHandler A consumer to handle circuit breaker triggering differently
     * @return the ClusteredPostingWriter instance
     */
    public CacheableClusteredPostingWriter getWriter(Consumer<Long> circuitBreakerHandler) {
        return new CacheClusteredPostingWriter(circuitBreakerHandler);
    }

    public ClusteredPostingCacheItem(CacheKey cacheKey) {
        this.cacheKey = cacheKey;
        CircuitBreakerManager.addWithoutBreaking(usedRamBytes.get());
    }

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    private class CacheClusteredPostingReader implements ClusteredPostingReader {
        @Override
        public PostingClusters read(BytesRef term) {
            PostingClusters clusters = clusteredPostings.get(term);
            if (clusters != null) {
                // Record access to update LRU status
                LruTermCache.TermKey termKey = new LruTermCache.TermKey(cacheKey, term.clone());
                LruTermCache.getInstance().updateAccess(termKey);
            }
            return clusters;
        }

        @Override
        public Set<BytesRef> getTerms() {
            // Note: We're returning the keySet directly instead of using Collections.unmodifiableSet()
            // for performance reasons. Callers should treat this as a read-only view.
            return clusteredPostings.keySet();
        }

        @Override
        public long size() {
            return clusteredPostings.size();
        }
    }

    private class CacheClusteredPostingWriter implements CacheableClusteredPostingWriter {

        private final Consumer<Long> circuitBreakerTriggerHandler;

        // Default handler: perform cache eviction when memory limit is reached
        private CacheClusteredPostingWriter() {
            this.circuitBreakerTriggerHandler = (ramBytesUsed) -> { LruTermCache.getInstance().evict(ramBytesUsed); };
        }

        private CacheClusteredPostingWriter(Consumer<Long> circuitBreakerTriggerHandler) {
            this.circuitBreakerTriggerHandler = circuitBreakerTriggerHandler;
        }

        @Override
        public void insert(BytesRef term, List<DocumentCluster> clusters) {
            if (clusters == null || clusters.isEmpty() || term == null) {
                return;
            }

            // Clone a new BytesRef object to avoid offset change
            BytesRef clonedTerm = term.clone();
            PostingClusters postingClusters = new PostingClusters(clusters);
            // BytesRef.bytes is never null
            long ramBytesUsed = postingClusters.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(clonedTerm) + clonedTerm.bytes.length;

            // TODO: avoid excessive log produced by circuit breaker
            if (!CircuitBreakerManager.addMemoryUsage(ramBytesUsed, CIRCUIT_BREAKER_LABEL)) {
                if (circuitBreakerTriggerHandler != null) {
                    circuitBreakerTriggerHandler.accept(ramBytesUsed);
                }

                // Try again after eviction
                if (!CircuitBreakerManager.addMemoryUsage(ramBytesUsed, CIRCUIT_BREAKER_LABEL)) {
                    log.warn("Failed to add to cache even after eviction, term will not be cached");
                    return;
                }
            }

            // Update the clusters with putIfAbsent for thread safety
            PostingClusters existingClusters = clusteredPostings.putIfAbsent(clonedTerm, postingClusters);
            // Record access to update LRU status
            LruTermCache.TermKey termKey = new LruTermCache.TermKey(cacheKey, clonedTerm);
            LruTermCache.getInstance().updateAccess(termKey);

            // Only update memory usage if we actually inserted a new entry
            if (existingClusters == null) {
                usedRamBytes.addAndGet(ramBytesUsed);
            } else {
                CircuitBreakerManager.releaseBytes(ramBytesUsed);
            }
        }

        @Override
        public long erase(BytesRef term) {
            if (term == null) {
                return 0;
            }
            PostingClusters postingClusters = clusteredPostings.get(term);
            if (postingClusters == null) {
                return 0;
            }
            // Clone a new BytesRef object to avoid offset change
            BytesRef clonedTerm = term.clone();
            long ramBytesReleased = postingClusters.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(clonedTerm) + clonedTerm.bytes.length;
            if (clusteredPostings.remove(clonedTerm) != null) {
                usedRamBytes.addAndGet(-ramBytesReleased);
                CircuitBreakerManager.releaseBytes(ramBytesReleased);
                return ramBytesReleased;
            }
            return 0;
        }
    }
}
