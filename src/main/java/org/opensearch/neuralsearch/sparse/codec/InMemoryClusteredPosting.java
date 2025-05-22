/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
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

/**
 * This class manages the in-memory postings for sparse vectors. It provides methods to write and read postings from memory.
 * It is used by the SparsePostingsConsumer and SparsePostingsReader classes.
 */
public class InMemoryClusteredPosting implements Accountable {
    public static final Map<InMemoryKey.IndexKey, Map<BytesRef, PostingClusters>> inMemoryPostings = new ConcurrentHashMap<>();

    public static void clearIndex(InMemoryKey.IndexKey key) {
        inMemoryPostings.remove(key);
    }

    @Override
    public long ramBytesUsed() {
        long ramUsed = 0;
        for (Map.Entry<InMemoryKey.IndexKey, Map<BytesRef, PostingClusters>> entry : inMemoryPostings.entrySet()) {
            ramUsed += RamUsageEstimator.shallowSizeOfInstance(InMemoryKey.IndexKey.class);
            for (Map.Entry<BytesRef, PostingClusters> entry2 : entry.getValue().entrySet()) {
                ramUsed += entry2.getKey().length;
                ramUsed += entry2.getValue().ramBytesUsed();
            }
        }
        return ramUsed;
    }

    @AllArgsConstructor
    public static class InMemoryClusteredPostingReader {
        private final InMemoryKey.IndexKey key;

        public PostingClusters read(BytesRef term) {
            return inMemoryPostings.getOrDefault(key, Collections.emptyMap()).get(term);
        }

        // once we enable cache eviction, this method will get partial data and should be removed.
        public Set<BytesRef> getTerms() {
            Map<BytesRef, PostingClusters> innerMap = inMemoryPostings.get(key);
            return innerMap != null ? innerMap.keySet() : Collections.emptySet();
        }

        public long size() {
            return inMemoryPostings.get(key).size();
        }
    }

    public static class InMemoryClusteredPostingWriter {
        public static Map<BytesRef, PostingClusters> writePostingClusters(
            InMemoryKey.IndexKey key,
            BytesRef term,
            List<DocumentCluster> clusters
        ) {
            if (clusters == null || clusters.isEmpty()) {
                return null;
            }
            return inMemoryPostings.compute(key, (k, existingMap) -> {
                if (existingMap == null) {
                    existingMap = new ConcurrentHashMap<>();
                }
                existingMap.put(term.clone(), new PostingClusters(clusters));
                return existingMap;
            });
        }
    }
}
