/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;

/**
 * Event listener for sparse index operations that handles cache cleanup during index removal.
 * Clears forward index and clustered posting caches for sparse token fields when indices are removed.
 */
@AllArgsConstructor
@Log4j2
public class SparseIndexEventListener implements IndexEventListener {
    @Override
    /**
     * This function is used to remove data from cache when index is removed.
     * The parameter reason is not used, because all kinds of its enum will have to go through this cache removing procedure.
     * @param indexService The index service for the removed index
     * @param reason The reason for the index removal
     */
    public void beforeIndexRemoved(IndexService indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {
        for (IndexShard shard : indexService) {
            try {
                MapperService mapperService = shard.mapperService();
                if (mapperService == null) {
                    // A closed index's IndexService is built without a MapperService
                    // (IndexService#needsMapperService), so a shard of one has no field types to
                    // look up, and its engine is closed so a segment snapshot would throw. Its
                    // caches were already cleared when the index was closed.
                    continue;
                }
                try (GatedCloseable<SegmentInfos> snapshot = shard.getSegmentInfosSnapshot()) {
                    SegmentInfos segmentInfos = snapshot.get();
                    for (int i = 0; i < segmentInfos.size(); i++) {
                        SegmentInfo segmentInfo = segmentInfos.info(i).info;
                        for (MappedFieldType fieldType : mapperService.fieldTypes()) {
                            if (fieldType instanceof SparseVectorFieldType) {
                                clearJvmCache(fieldType.name(), segmentInfo);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Never propagate: throwing here aborts IndicesService#removeIndex before it
                // releases the shard lock, and the reopened shard then stays unassigned forever. A
                // cache entry that outlives its index is the lesser of the two failures, and it is
                // still evicted when its own segment closes.
                log.error("An error occurred during remove index from cache", e);
            }
        }
    }

    private void clearJvmCache(String fieldName, SegmentInfo segmentInfo) {
        CacheKey key = new CacheKey(segmentInfo, fieldName);
        ForwardIndexCache.getInstance().onIndexRemoval(key);
        ClusteredPostingCache.getInstance().onIndexRemoval(key);
    }
}
