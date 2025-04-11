/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.AllArgsConstructor;
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

@AllArgsConstructor
public class SparseIndexEventListener implements IndexEventListener {
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
                            ForwardIndexCache.getInstance().removeIndex(key);
                            ClusteredPostingCache.getInstance().removeIndex(key);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
