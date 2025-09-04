/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.Before;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;

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
