/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.SegmentNativeIndex;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_BLOCK_BUDGET;

/**
 * Scores one segment against the native engine index.
 *
 * nsparse has no cursor: the whole top-k comes back from a {@code queryIndex} call in the
 * constructor, and this scorer then just replays those hits in doc order. So unlike the Lucene
 * path, k is decided before iteration and the collector cannot make the query stop early.
 *
 * Filters are passed down as a candidate set for nsparse to restrict its search to; deletions are
 * not, and are dropped from the returned hits instead -- which can leave the fetch short of k and
 * send it back to nsparse with a larger one.
 */
@Log4j2
public class NativeIndexScorer extends Scorer {
    /**
     * nsparse's selector kind for the filter ids: 0 builds a SetIDSelector, 1 an ArrayIDSelector.
     */
    private static final int FILTER_IDS_TYPE_SET = 0;

    private final ResultsDocValueIterator<Float> resultsIterator;
    /**
     * nsparse decodes its own quantization, so unlike the Lucene path there is no rescaling to
     * fold the boost into -- it multiplies the returned score and nothing else.
     */
    private final float boost;

    /**
     * Creates scorer with upfront search results and optional filtering.
     */
    public NativeIndexScorer(
        FieldInfo fieldInfo,
        SparseQueryContext sparseQueryContext,
        Map<Integer, Float> rawQueryVector,
        LeafReader leafReader,
        SegmentInfo segmentInfo,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        float boost
    ) throws IOException {
        this.boost = boost;
        StopWatch searchUpfrontStopWatch = StopWatch.createStarted();
        List<Pair<Integer, Float>> results = searchUpfront(
            sparseQueryContext,
            fieldInfo,
            rawQueryVector,
            sparseQueryContext.getK(),
            acceptedDocs,
            filterBitSetIterator,
            leafReader,
            segmentInfo
        );
        int maxDoc = segmentInfo.maxDoc();
        for (Pair<Integer, Float> result : results) {
            if (result.getKey() >= maxDoc) {
                log.error("docId: {}, maxDoc: {}", result.getKey(), maxDoc);
            }
        }
        searchUpfrontStopWatch.stop();
        log.debug("searchUpfront took {} ms", searchUpfrontStopWatch.getTime(TimeUnit.MILLISECONDS));
        resultsIterator = new ResultsDocValueIterator<>(results);
    }

    private List<Pair<Integer, Float>> searchUpfront(
        SparseQueryContext sparseQueryContext,
        FieldInfo fieldInfo,
        Map<Integer, Float> rawQueryVector,
        int resultSize,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        LeafReader leafReader,
        SegmentInfo segmentInfo
    ) throws IOException {
        int[] tokens = Ints.toArray(rawQueryVector.keySet());
        float[] weights = Floats.toArray(rawQueryVector.values());
        Map<String, Object> searchParameters = buildSearchParameters(sparseQueryContext, segmentInfo, fieldInfo);

        StopWatch loadIndexStopWatch = StopWatch.createStarted();
        try (SegmentNativeIndex nativeIndex = SegmentNativeIndex.open(leafReader, segmentInfo, fieldInfo.getName())) {
            long indexAddress = nativeIndex.address();
            loadIndexStopWatch.stop();
            log.debug("loadIndex took {} ms", loadIndexStopWatch.getTime(TimeUnit.MILLISECONDS));

            // Only a query filter is a candidate set. Live docs are a mask: handing them to nsparse as a
            // filter would make every query on a segment with a single deletion an enumeration of every
            // live doc, and once the live count drops to <= k nsparse switches to an exact match that
            // returns all of them -- including docs sharing no token with the query, at score 0. Lucene
            // skips deleted docs while traversing postings instead, so drop them from the results below.
            final long[] docsIds = filterBitSetIterator == null ? null : constructFilterList(acceptedDocs, filterBitSetIterator);
            final boolean maskDeletedDocs = docsIds == null && acceptedDocs != null;
            // maxDoc covers every doc in the segment, so no fetch can usefully ask for more.
            final int maxFetchSize = segmentInfo.maxDoc();

            // Deleted hits are dropped after the fact, so a fetch of exactly k can come back short.
            // Grow and retry rather than budget for every deletion upfront: numDeletedDocs() is
            // segment-wide, so k + numDeletedDocs() turns a k=10 query on a segment with 10K deletions
            // into a top-10010 -- and in seismic a larger k also raises the heap's admission threshold
            // more slowly, so it visits far more blocks. Deletions rarely reach the top k, so the common
            // case pays exactly k and the loop runs once.
            int fetchSize = maskDeletedDocs ? Math.min(resultSize, maxFetchSize) : resultSize;
            List<Pair<Integer, Float>> hits;
            while (true) {
                SparseQueryResult[] results = queryNative(indexAddress, tokens, weights, fetchSize, searchParameters, docsIds);
                Stream<Pair<Integer, Float>> stream = Arrays.stream(results).map(r -> Pair.of(r.getId(), r.getScore()));
                if (maskDeletedDocs) {
                    stream = stream.filter(hit -> acceptedDocs.get(hit.getLeft())).limit(resultSize);
                }
                hits = stream.toList();
                // A short result set means nsparse had nothing more to give at this fetch size, so a
                // bigger one cannot fill the gap either.
                if (maskDeletedDocs == false || hits.size() >= resultSize || results.length < fetchSize || fetchSize >= maxFetchSize) {
                    break;
                }
                fetchSize = nextFetchSize(fetchSize, hits.size(), resultSize, maxFetchSize);
            }
            // nsparse returns hits in score order, but a DocIdSetIterator must walk doc IDs in ascending
            // order. Emitting them by score trips a Lucene assertion as soon as the scorer is wrapped in
            // a conjunction -- a nested query does that, and the assertion kills the node. The score order
            // has to survive until after the limit above, which keeps the k best rather than k arbitrary.
            return hits.stream().sorted(Comparator.comparingInt(Pair::getLeft)).toList();
        } catch (Exception e) {
            log.error("search parameters: {}", searchParameters);
            throw e;
        }
    }

    private SparseQueryResult[] queryNative(
        long indexAddress,
        int[] tokens,
        float[] weights,
        int fetchSize,
        Map<String, Object> searchParameters,
        long[] docsIds
    ) {
        if (docsIds == null) {
            return NativeLibrary.queryIndex(indexAddress, tokens, weights, fetchSize, searchParameters);
        }
        return NativeLibrary.queryIndexWithFilter(indexAddress, tokens, weights, fetchSize, searchParameters, docsIds, FILTER_IDS_TYPE_SET);
    }

    /**
     * Grows the fetch to at least what the surviving fraction implies is needed, so a segment whose top
     * hits are mostly deleted converges in a call or two instead of doubling from k up to maxDoc.
     * Doubling is the floor, which keeps the growth geometric when nothing survived to extrapolate from.
     */
    private static int nextFetchSize(int fetchSize, int survivors, int resultSize, int maxFetchSize) {
        long doubled = (long) fetchSize * 2;
        long projected = survivors == 0 ? doubled : (long) Math.ceil((double) fetchSize * resultSize / survivors);
        return (int) Math.min(maxFetchSize, Math.max(doubled, projected));
    }

    private long[] constructFilterList(Bits acceptedDocs, BitSetIterator filterBitSetIterator) throws IOException {
        // SparseVectorQuery.createBitSet already intersects the filter with live docs; this is belt and
        // braces for a filter bitset that reached us without that step.
        DocIdSetIterator iterator = (acceptedDocs != null)
            ? new FilteredBitSetIterator(filterBitSetIterator, acceptedDocs)
            : filterBitSetIterator;

        // cost() is the filter bitset's cardinality, so it is exact unless the live-doc mask above
        // drops something -- an upper bound either way, and the array is trimmed to fit at the end.
        long[] docIds = new long[Math.max(1, (int) Math.min(iterator.cost(), Integer.MAX_VALUE))];
        int size = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docIds = ArrayUtil.grow(docIds, size + 1);
            docIds[size++] = doc;
        }
        return size == docIds.length ? docIds : ArrayUtil.copyOfSubArray(docIds, 0, size);
    }

    /**
     * Iterates a BitSetIterator while also checking acceptedDocs (liveDocs).
     */
    private static class FilteredBitSetIterator extends DocIdSetIterator {
        private final BitSetIterator delegate;
        private final Bits acceptedDocs;

        FilteredBitSetIterator(BitSetIterator delegate, Bits acceptedDocs) {
            this.delegate = delegate;
            this.acceptedDocs = acceptedDocs;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc;
            while ((doc = delegate.nextDoc()) != NO_MORE_DOCS) {
                if (acceptedDocs.get(doc)) {
                    return doc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
            if (acceptedDocs.get(doc)) return doc;
            return nextDoc();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    private Map<String, Object> buildSearchParameters(SparseQueryContext sparseQueryContext, SegmentInfo segmentInfo, FieldInfo fieldInfo) {
        Map<String, Object> searchParameters = new HashMap<>();
        if (PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
            searchParameters.put("cut", sparseQueryContext.getTokens().size());
            searchParameters.put("heap_factor", sparseQueryContext.getHeapFactor());
            // vmin/vmax select a quantized SearchParameters subtype, which encodes the query at
            // the search ceiling instead of the index's ingest one. Without them the quantized
            // index falls back to its own doc-side quantizer, clamping every query weight
            // above the ingest ceiling to the top code.
            searchParameters.put("vmin", 0.0f);
            searchParameters.put("vmax", ByteQuantizationUtil.getCeilingValueSearch(fieldInfo));
            if (SparseFieldUtils.getSparseForwardIndex(fieldInfo) == SparseForwardIndex.PER_BLOCK) {
                // k_prime is what makes the range above land on DiskSeismicSQSearchParameters,
                // the only subtype a disk_seismic_sq index reads a query range from. Pinned to
                // nsparse's own default until it is exposed as a query parameter, so a per_block
                // field quantizes like a shared one instead of at its ingest ceiling.
                searchParameters.put("k_prime", DEFAULT_BLOCK_BUDGET);
            }
        }
        return searchParameters;
    }

    @Override
    public int docID() {
        return resultsIterator.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return resultsIterator;
    }

    /**
     * Returns maximum possible score up to given document ID.
     */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    /**
     * Computes score for current document using similarity scorer.
     */
    @Override
    public float score() throws IOException {
        return boost * resultsIterator.score();
    }
}
