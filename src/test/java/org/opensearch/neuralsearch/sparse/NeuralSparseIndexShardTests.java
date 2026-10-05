/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeuralSparseIndexShardTests extends AbstractSparseTestBase {

    private IndexShard indexShard;
    private Engine.Searcher searcher;
    private NeuralSparseIndexShard neuralSparseIndexShard;
    private String expectedIndexName;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        expectedIndexName = "test-index";
        Index testIndex = new Index(expectedIndexName, "uuid");
        ShardId testShardId = new ShardId(testIndex, 0);

        indexShard = mock(IndexShard.class);
        searcher = mock(Engine.Searcher.class);

        when(indexShard.shardId()).thenReturn(testShardId);
    }

    public void testGetIndexName() {
        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        String actualIndexName = neuralSparseIndexShard.getIndexName();

        // Verify
        assertEquals(expectedIndexName, actualIndexName);
        verify(indexShard).shardId();
    }

    public void testWarmUpWithSparseFields() throws IOException {
        // Setup with proper sparse field
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(TestsPrepareUtils.prepareIndexReaderWithSparseField(15));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.warmUp();

        // Verify
        verify(indexShard).acquireSearcher("warm-up-searcher-source");
        verify(searcher).close();
    }

    public void testClearCacheWithSparseFields() throws IOException {
        // Setup with proper sparse field
        when(indexShard.acquireSearcher("clear-cache-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(TestsPrepareUtils.prepareIndexReaderWithSparseField(15));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.clearCache();

        // Verify
        verify(indexShard).acquireSearcher("clear-cache-searcher-source");
        verify(searcher).close();
    }

    public void testWarmUpSkipsNativeEngineField() throws IOException {
        DirectoryReader reader = TestsPrepareUtils.prepareIndexReaderWithSparseField(15, SparseEngine.NATIVE);
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(reader);

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.warmUp();

        // Verify nothing was loaded: the native engine reads its own index file, not these caches
        CacheKey cacheKey = cacheKeyOf(reader);
        assertNull(ForwardIndexCache.getInstance().get(cacheKey));
        assertNull(ClusteredPostingCache.getInstance().get(cacheKey));
        verify(searcher).close();
    }

    public void testWarmUpLoadsLuceneEngineField() throws IOException {
        DirectoryReader reader = TestsPrepareUtils.prepareIndexReaderWithSparseField(15, SparseEngine.LUCENE);
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(reader);

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.warmUp();

        // Verify: the same reader on the Lucene engine does populate the caches the native one skips
        CacheKey cacheKey = cacheKeyOf(reader);
        assertNotNull(ForwardIndexCache.getInstance().get(cacheKey));
        assertNotNull(ClusteredPostingCache.getInstance().get(cacheKey));
    }

    public void testClearCacheSkipsNativeEngineField() throws IOException {
        DirectoryReader reader = TestsPrepareUtils.prepareIndexReaderWithSparseField(15, SparseEngine.NATIVE);
        when(indexShard.acquireSearcher("clear-cache-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(reader);

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.clearCache();

        // Verify: a no-op that reports success, rather than a shard failure
        verify(indexShard).acquireSearcher("clear-cache-searcher-source");
        verify(searcher).close();
    }

    /**
     * The cache key the shard builds for the reader's only sparse field. SegmentInfo has no equals, so
     * it has to be the very instance the shard sees.
     */
    private CacheKey cacheKeyOf(DirectoryReader reader) {
        LeafReader leafReader = reader.leaves().get(0).reader();
        SegmentInfo segmentInfo = Lucene.segmentReader(leafReader).getSegmentInfo().info;
        return new CacheKey(segmentInfo, leafReader.getFieldInfos().fieldInfo("sparse_field"));
    }

    public void testWarmUpWithPredicateFailure() throws IOException {
        // Setup with sparse field that fails predicate test (threshold too high)
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(TestsPrepareUtils.prepareIndexReaderWithSparseField(5));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.warmUp();

        // Verify
        verify(indexShard).acquireSearcher("warm-up-searcher-source");
        verify(searcher).close();
    }

    public void testClearCacheWithPredicateFailure() throws IOException {
        // Setup with sparse field that fails predicate test (threshold too high)
        when(indexShard.acquireSearcher("clear-cache-searcher-source")).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(TestsPrepareUtils.prepareIndexReaderWithSparseField(5));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute
        neuralSparseIndexShard.clearCache();

        // Verify
        verify(indexShard).acquireSearcher("clear-cache-searcher-source");
        verify(searcher).close();
    }

    public void testWarmUpThrowsIllegalIndexShardStateException() throws IOException {
        // Setup to throw IllegalIndexShardStateException when acquiring searcher
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenThrow(new IllegalIndexShardStateException(new ShardId("test", "uuid", 0), null, "test exception"));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute and verify exception is thrown
        expectThrows(IllegalIndexShardStateException.class, () -> neuralSparseIndexShard.warmUp());
        verify(indexShard).acquireSearcher("warm-up-searcher-source");
    }

    public void testWarmUpThrowsEngineException() throws IOException {
        // Setup to throw EngineException when acquiring searcher
        when(indexShard.acquireSearcher("warm-up-searcher-source")).thenThrow(new EngineException(new ShardId("test", "uuid", 0), "test engine exception"));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute and verify exception is thrown
        expectThrows(EngineException.class, () -> neuralSparseIndexShard.warmUp());
        verify(indexShard).acquireSearcher("warm-up-searcher-source");
    }

    public void testClearCacheThrowsIllegalIndexShardStateException() throws IOException {
        // Setup to throw IllegalIndexShardStateException when acquiring searcher
        when(indexShard.acquireSearcher("clear-cache-searcher-source")).thenThrow(new IllegalIndexShardStateException(new ShardId("test", "uuid", 0), null, "test exception"));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute and verify exception is thrown
        expectThrows(IllegalIndexShardStateException.class, () -> neuralSparseIndexShard.clearCache());
        verify(indexShard).acquireSearcher("clear-cache-searcher-source");
    }

    public void testClearCacheThrowsEngineException() throws IOException {
        // Setup to throw EngineException when acquiring searcher
        when(indexShard.acquireSearcher("clear-cache-searcher-source")).thenThrow(new EngineException(new ShardId("test", "uuid", 0), "test engine exception"));

        neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);

        // Execute and verify exception is thrown
        expectThrows(EngineException.class, () -> neuralSparseIndexShard.clearCache());
        verify(indexShard).acquireSearcher("clear-cache-searcher-source");
    }
}
