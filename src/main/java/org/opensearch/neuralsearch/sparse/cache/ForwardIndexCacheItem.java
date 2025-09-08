/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * This class stores sparse vector in cache and provides read/write operations.
 */
@Log4j2
public class ForwardIndexCacheItem extends AccountableTracker implements SparseVectorForwardIndex {

    private final CacheKey cacheKey;
    private final AtomicReferenceArray<SparseVector> sparseVectors;
    private final RamBytesRecorder globalRamBytes;
    @Getter
    private final SparseVectorReader reader = new CacheSparseVectorReader();
    @Getter
    private final CacheableSparseVectorWriter writer = new CacheSparseVectorWriter();

    /**
     * Returns the writer instance.
     * @param circuitBreakerHandler A consumer to handle circuit breaker triggering differently
     * @return the SparseVectorWriter instance
     */
    public CacheableSparseVectorWriter getWriter(Consumer<Long> circuitBreakerHandler) {
        return new CacheSparseVectorWriter(circuitBreakerHandler);
    }

    public ForwardIndexCacheItem(CacheKey cacheKey, int docCount, RamBytesRecorder globalRamBytes) {
        this.cacheKey = cacheKey;
        this.globalRamBytes = globalRamBytes;
        sparseVectors = new AtomicReferenceArray<>(docCount);
        // Account for the array itself in memory usage
        recordUsedBytes(
            RamUsageEstimator.shallowSizeOf(sparseVectors) + RamUsageEstimator.alignObjectSize(
                (long) docCount * RamUsageEstimator.NUM_BYTES_OBJECT_REF
            )
        );
        globalRamBytes.safeRecord(ramBytesUsed(), CircuitBreakerManager::addWithoutBreaking);
    }

    private class CacheSparseVectorReader implements SparseVectorReader {
        @Override
        public SparseVector read(int docId) throws IOException {
            if (docId < 0 || docId >= sparseVectors.length()) {
                return null;
            }
            SparseVector vector = sparseVectors.get(docId);
            if (vector != null) {
                // Record access to update LRU status
                LruDocumentCache.DocumentKey documentKey = new LruDocumentCache.DocumentKey(cacheKey, docId);
                LruDocumentCache.getInstance().updateAccess(documentKey);
            }
            return vector;
        }
    }

    private class CacheSparseVectorWriter implements CacheableSparseVectorWriter {

        private final Consumer<Long> circuitBreakerTriggerHandler;

        // Default handler: perform cache eviction when memory limit is reached
        private CacheSparseVectorWriter() {
            this.circuitBreakerTriggerHandler = (ramBytesUsed) -> { LruDocumentCache.getInstance().evict(ramBytesUsed); };
        }

        private CacheSparseVectorWriter(Consumer<Long> circuitBreakerTriggerHandler) {
            this.circuitBreakerTriggerHandler = circuitBreakerTriggerHandler;
        }

        @Override
        public void insert(int docId, SparseVector vector) {
            if (vector == null || docId < 0 || docId >= sparseVectors.length() || sparseVectors.get(docId) != null) {
                return;
            }

            long ramBytesUsed = vector.ramBytesUsed();

            if (!globalRamBytes.record(ramBytesUsed)) {
                if (circuitBreakerTriggerHandler != null) {
                    circuitBreakerTriggerHandler.accept(ramBytesUsed);
                    // Try again after eviction
                    if (!globalRamBytes.record(ramBytesUsed)) {
                        return;
                    }
                }
            }

            // Record access to update LRU status
            LruDocumentCache.DocumentKey documentKey = new LruDocumentCache.DocumentKey(cacheKey, docId);
            LruDocumentCache.getInstance().updateAccess(documentKey);

            // Only update memory usage if we actually inserted a new document
            if (sparseVectors.compareAndSet(docId, null, vector)) {
                recordUsedBytes(ramBytesUsed);
            } else {
                globalRamBytes.safeRecord(-ramBytesUsed, CircuitBreakerManager::addWithoutBreaking);
            }
        }

        /**
         * Removes a sparse vector from the cached forward index.
         *
         * @param docId The document ID of the sparse vector to be removed from the forward index
         * @return The number of RAM bytes freed by removing this sparse vector
         */
        @Override
        public long erase(int docId) {
            if (docId < 0 || docId >= sparseVectors.length()) {
                return 0;
            }

            SparseVector vector = sparseVectors.get(docId);
            if (vector == null) {
                return 0;
            }

            long ramBytesReleased = vector.ramBytesUsed();

            // Only update memory usage if we actually erased a new document
            if (sparseVectors.compareAndSet(docId, vector, null)) {
                recordUsedBytes(-ramBytesReleased);
                globalRamBytes.safeRecord(-ramBytesReleased, CircuitBreakerManager::addWithoutBreaking);
                return ramBytesReleased;
            }

            return 0;
        }
    }
}
