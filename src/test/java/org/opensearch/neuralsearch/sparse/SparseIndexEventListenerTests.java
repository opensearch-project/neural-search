/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseIndexEventListenerTests extends AbstractSparseTestBase {

    private SparseIndexEventListener listener;

    @Mock
    private IndexService indexService;
    @Mock
    private IndexShard indexShard;
    @Mock
    private MapperService mapperService;
    @Mock
    private SegmentInfos segmentInfos;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new SparseIndexEventListener();
    }

    public void testBeforeIndexRemoved_withSparseVectorField_clearsCache() throws IOException {
        SegmentCommitInfo segmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        SparseVectorFieldType sparseFieldType = mock(SparseVectorFieldType.class);
        when(sparseFieldType.name()).thenReturn("sparse_field");

        GatedCloseable<SegmentInfos> gatedCloseable = mock(GatedCloseable.class);
        when(gatedCloseable.get()).thenReturn(segmentInfos);

        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard).iterator());
        when(indexShard.mapperService()).thenReturn(mapperService);
        when(indexShard.getSegmentInfosSnapshot()).thenReturn(gatedCloseable);
        when(segmentInfos.size()).thenReturn(1);
        when(segmentInfos.info(0)).thenReturn(segmentCommitInfo);
        when(mapperService.fieldTypes()).thenReturn(Arrays.asList(sparseFieldType));

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED);

        verify(gatedCloseable).close();
    }

    public void testBeforeIndexRemoved_withNonSparseField_doesNotClearCache() throws IOException {
        SegmentCommitInfo segmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        MappedFieldType regularFieldType = mock(MappedFieldType.class);
        when(regularFieldType.name()).thenReturn("regular_field");

        GatedCloseable<SegmentInfos> gatedCloseable = mock(GatedCloseable.class);
        when(gatedCloseable.get()).thenReturn(segmentInfos);

        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard).iterator());
        when(indexShard.mapperService()).thenReturn(mapperService);
        when(indexShard.getSegmentInfosSnapshot()).thenReturn(gatedCloseable);
        when(segmentInfos.size()).thenReturn(1);
        when(segmentInfos.info(0)).thenReturn(segmentCommitInfo);
        when(mapperService.fieldTypes()).thenReturn(Arrays.asList(regularFieldType));

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED);

        verify(gatedCloseable).close();
    }

    public void testBeforeIndexRemoved_withEmptySegments_handlesGracefully() throws IOException {
        GatedCloseable<SegmentInfos> gatedCloseable = mock(GatedCloseable.class);
        when(gatedCloseable.get()).thenReturn(segmentInfos);

        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard).iterator());
        when(indexShard.mapperService()).thenReturn(mapperService);
        when(indexShard.getSegmentInfosSnapshot()).thenReturn(gatedCloseable);
        when(segmentInfos.size()).thenReturn(0);

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED);

        verify(gatedCloseable).close();
    }

    public void testBeforeIndexRemoved_withNullMapperService_skipsShardWithoutSnapshot() throws IOException {
        // A closed index's shards have no MapperService, and their engine is closed, so asking for
        // a segment snapshot would throw AlreadyClosedException.
        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard).iterator());
        when(indexShard.mapperService()).thenReturn(null);

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.REOPENED);

        verify(indexShard, never()).getSegmentInfosSnapshot();
    }

    public void testBeforeIndexRemoved_withException_doesNotPropagate() throws IOException {
        // Throwing aborts IndicesService#removeIndex before it releases the shard lock, which
        // leaves a reopened shard unassigned. Every failure here has to stay logged and swallowed.
        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard, indexShard).iterator());
        when(indexShard.mapperService()).thenThrow(new RuntimeException("Test exception"));

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED);

        // The second shard is still visited rather than skipped by an escaping exception.
        verify(indexShard, times(2)).mapperService();
    }

    public void testBeforeIndexRemoved_withClosedEngine_doesNotPropagate() throws IOException {
        when(indexService.iterator()).thenReturn(Arrays.asList(indexShard).iterator());
        when(indexShard.mapperService()).thenReturn(mapperService);
        when(indexShard.getSegmentInfosSnapshot()).thenThrow(new AlreadyClosedException("engine is closed"));

        listener.beforeIndexRemoved(indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.REOPENED);

        verify(indexShard).getSegmentInfosSnapshot();
    }
}
