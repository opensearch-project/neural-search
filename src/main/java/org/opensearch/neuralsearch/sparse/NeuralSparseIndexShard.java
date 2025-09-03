/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.CodecUtilWrapper;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.codec.SparseTermsLuceneReader;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensField;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader.NOOP_READER;

/**
 * NeuralSparseIndexShard wraps IndexShard and adds methods to perform neural-sparse related operations against the shard
 */
@Log4j2
@RequiredArgsConstructor
public class NeuralSparseIndexShard {
    @Getter
    @NonNull
    private final IndexShard indexShard;

    private static final String WARM_UP_SEARCHER_SOURCE = "warm-up-searcher-source";
    private static final String CLEAR_CACHE_SEARCHER_SOURCE = "clear-cache-searcher-source";

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    /**
     * Load all the neural-sparse segments for this shard into the cache.
     * Preloads sparse field data to improve query performance.
     * Early stop to save resources if this is a repeated request
     */
    public void warmUp() throws IOException {
        try (Engine.Searcher searcher = indexShard.acquireSearcher(WARM_UP_SEARCHER_SOURCE)) {
            List<CacheOperationContext> cacheOperationContexts = collectCacheOperationContexts(searcher);

            // Fist warm up all forward indices
            warmUpAllForwardIndices(cacheOperationContexts);

            // Then warm up all clustered postings
            warmUpAllClusteredPostings(cacheOperationContexts);
        } catch (IllegalIndexShardStateException | EngineException e) {
            log.error("[Neural Sparse] Failed to acquire searcher", e);
            throw e;
        } catch (CircuitBreakingException e) {
            log.error("[Neural Sparse] Circuit Breaker reaches limit", e);
            throw e;
        } catch (IOException e) {
            log.error("[Neural Sparse] Failed to read data during warm up", e);
            throw e;
        }
    }

    /**
     * Clear all cached neural-sparse data for this shard.
     * Removes sparse field data from memory to free up resources.
     */
    public void clearCache() throws IOException {
        try (Engine.Searcher searcher = indexShard.acquireSearcher(CLEAR_CACHE_SEARCHER_SOURCE)) {
            List<CacheOperationContext> cacheOperationContexts = collectCacheOperationContexts(searcher);

            // Clear cache for all collected contexts
            clearAllCaches(cacheOperationContexts);
        } catch (IllegalIndexShardStateException | EngineException e) {
            log.error("[Neural Sparse] Failed to acquire searcher", e);
            throw e;
        } catch (IOException e) {
            log.error("[Neural Sparse] Failed to read data during cache clearing", e);
            throw e;
        }
    }

    /**
     * Warm up all forward indices
     */
    private void warmUpAllForwardIndices(List<CacheOperationContext> contexts) throws IOException, CircuitBreakingException {
        for (CacheOperationContext context : contexts) {
            BinaryDocValues binaryDocValues = context.binaryDocValues;
            SparseVectorReader forwardIndexReader = context.forwardIndexReader;
            if (forwardIndexReader == null) {
                continue;
            }

            int docId = binaryDocValues.nextDoc();
            while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                forwardIndexReader.read(docId);
                docId = binaryDocValues.nextDoc();
            }
        }
    }

    /**
     * Warm up all clustered postings
     */
    private void warmUpAllClusteredPostings(List<CacheOperationContext> contexts) throws IOException, CircuitBreakingException {
        for (CacheOperationContext context : contexts) {
            CacheGatedPostingsReader postingsReader = context.postingsReader;

            final Set<BytesRef> terms = postingsReader.getTerms();
            for (BytesRef term : terms) {
                postingsReader.read(term);
            }
        }
    }

    /**
     * Clear caches for all collected contexts
     */
    private void clearAllCaches(List<CacheOperationContext> contexts) {
        for (CacheOperationContext context : contexts) {
            CacheKey cacheKey = context.cacheKey;
            ClusteredPostingCache.getInstance().removeIndex(cacheKey);
            ForwardIndexCache.getInstance().removeIndex(cacheKey);
        }
    }

    private SparseVectorReader getCacheGatedForwardIndexReader(BinaryDocValues binaryDocValues, CacheKey key, int docCount) {
        if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
            return NOOP_READER;
        }
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues = (SparseBinaryDocValuesPassThrough) binaryDocValues;
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(key, docCount);
        return new CacheGatedForwardIndexReader(
            cacheItem.getReader(),
            cacheItem.getWriter(this::customizedConsumer),
            sparseBinaryDocValues
        );
    }

    private CacheGatedPostingsReader getCacheGatedPostingReader(FieldInfo fieldInfo, CacheKey key, SegmentInfo segmentInfo)
        throws IOException {
        final SparseTermsLuceneReader luceneReader = new SparseTermsLuceneReader(
            createSegmentReadState(segmentInfo),
            new CodecUtilWrapper()
        );
        return new CacheGatedPostingsReader(
            fieldInfo.name,
            ClusteredPostingCache.getInstance().getOrCreate(key).getReader(),
            ClusteredPostingCache.getInstance().getOrCreate(key).getWriter(this::customizedConsumer),
            luceneReader
        );
    }

    private void customizedConsumer(long ramBytesUsed) {
        throw new CircuitBreakingException("Circuit Breaker reaches limit", CircuitBreaker.Durability.PERMANENT);
    }

    private Set<FieldInfo> collectSparseFieldInfos(LeafReader leafReader) {
        return StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
            .filter(SparseTokensField::isSparseField)
            .collect(Collectors.toSet());
    }

    private SegmentReadState createSegmentReadState(SegmentInfo segmentInfo) throws IOException {
        final Codec codec = segmentInfo.getCodec();
        final Directory cfsDir;
        final FieldInfos coreFieldInfos;

        if (segmentInfo.getUseCompoundFile()) {
            // If we get compound file, we will set directory as csf file
            cfsDir = codec.compoundFormat().getCompoundReader(segmentInfo.dir, segmentInfo);
        } else {
            // Otherwise, we set directory as dir coming from segmentInfo
            cfsDir = segmentInfo.dir;
        }
        coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, segmentInfo, "", IOContext.DEFAULT);

        return new SegmentReadState(cfsDir, segmentInfo, coreFieldInfos, IOContext.DEFAULT);
    }

    /**
     * Collect contexts needed during cache operation
     */
    private List<CacheOperationContext> collectCacheOperationContexts(Engine.Searcher searcher) throws IOException {
        List<CacheOperationContext> contexts = new ArrayList<>();

        for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
            final LeafReader leafReader = leafReaderContext.reader();
            final Set<FieldInfo> sparseFieldInfos = collectSparseFieldInfos(leafReader);
            final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
            final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;

            for (FieldInfo fieldInfo : sparseFieldInfos) {
                if (!PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
                    continue;
                }
                final CacheKey key = new CacheKey(segmentInfo, fieldInfo);

                final BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldInfo.name);
                SparseVectorReader forwardIndexReader;
                if (binaryDocValues == null) {
                    log.error("[Neural Sparse] No binary doc values found for field: {}", fieldInfo.name);
                    forwardIndexReader = null;
                } else {
                    forwardIndexReader = getCacheGatedForwardIndexReader(binaryDocValues, key, segmentInfo.maxDoc());
                }

                final CacheGatedPostingsReader postingsReader = getCacheGatedPostingReader(fieldInfo, key, segmentInfo);

                contexts.add(new CacheOperationContext(binaryDocValues, forwardIndexReader, postingsReader, key));
            }
        }

        return contexts;
    }

    /**
     * Used to store related context information during warm up or clear cache
     */
    private static class CacheOperationContext {
        final BinaryDocValues binaryDocValues;
        final SparseVectorReader forwardIndexReader;
        final CacheGatedPostingsReader postingsReader;
        final CacheKey cacheKey;

        CacheOperationContext(
            BinaryDocValues binaryDocValues,
            SparseVectorReader forwardIndexReader,
            CacheGatedPostingsReader postingsReader,
            CacheKey key
        ) {
            this.binaryDocValues = binaryDocValues;
            this.forwardIndexReader = forwardIndexReader;
            this.postingsReader = postingsReader;
            this.cacheKey = key;
        }
    }
}
