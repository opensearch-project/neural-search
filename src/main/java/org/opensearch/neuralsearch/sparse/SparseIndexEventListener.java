/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;

/**
 * Event listener for sparse index operations that handles cache cleanup during index removal.
 * Clears forward index and clustered posting caches for sparse token fields when indices are removed.
 */
@AllArgsConstructor
@Log4j2
public class SparseIndexEventListener implements IndexEventListener {
    @Override
    public void beforeIndexRemoved(IndexService indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {
        for (IndexShard shard : indexService) {
            try (MapperService mapperService = shard.mapperService()) {
                SegmentInfos segmentInfos = shard.getSegmentInfosSnapshot().get();
                for (int i = 0; i < segmentInfos.size(); i++) {
                    SegmentInfo segmentInfo = segmentInfos.info(i).info;
                    for (MappedFieldType fieldType : mapperService.fieldTypes()) {
                        if (fieldType instanceof SparseTokensFieldType) {
                            String fieldName = fieldType.name();
                            CacheKey key = new CacheKey(segmentInfo, fieldName);
                            ForwardIndexCache.getInstance().onIndexRemoval(key);
                            ClusteredPostingCache.getInstance().onIndexRemoval(key);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("An error occurred during remove index from cache", e);
                throw new RuntimeException(e);
            }
        }
    }
}
