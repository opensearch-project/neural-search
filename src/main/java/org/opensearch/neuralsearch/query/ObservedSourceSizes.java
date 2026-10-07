/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * How many bytes of {@code _source} does a returned document actually carry, per index and per {@code _source} filter
 * shape — learned from the responses that go past the fused-mode filter, for the fast path's fetch-volume gate.
 *
 * <p>{@link ReturnedEmbeddingFields} estimates the fetch volume of a fast-path request from the mapping, and the mapping
 * can only speak for the fields whose size it declares: a {@code knn_vector}'s dimension, a {@code rank_features}. Text is
 * invisible to it. A corpus of 50 KB articles with no vector field estimates to zero bytes, arms the fast path at any
 * window, and has its legs fetch {@code legs × window} documents — 190 for a {@code size:10, window:100} request — where
 * round 2 would have fetched ten: ~9.5 MB against ~0.5 MB, more than the round saved is worth by a wide margin, and on the
 * <b>success</b> path, not only when the count fails to settle. What decides the trade is the size of the document as
 * returned, filters applied, and the only place that number exists is a response.
 *
 * <p>So it is taken from responses. {@code HybridQuerySearchRequestFilter} handles every response of a request whose
 * shape the fast path could answer — on both paths, since the two-round page carries the same {@code _source} the fast
 * path's would — and records the mean serialized {@code _source} bytes per returned hit here, keyed by the hit's index
 * and by the request's {@code _source} includes/excludes in canonical form (the shard applies that filter before
 * serializing, so the same document is a different size under a different filter). The gate then multiplies the
 * observed bytes by the documents it would over-fetch. Unobserved means unknown, and unknown fails closed: the request
 * takes the two-round path it always had, and its own page is the first observation — one two-round request per index
 * and filter shape, per coordinator node.
 *
 * <p>The value is an exponentially weighted mean of page means ({@link #EWMA_WEIGHT} on the newest page), so it follows
 * a corpus whose documents grow or shrink and is not dominated by one unusual page; it is biased toward the documents
 * that rank at the top, which is what the gate is about. This is a latency predictor, never a correctness input — a
 * wrong number can only send a request down the slower of two paths that return the same page — so per-node state and an
 * estimate that lags a changing corpus are acceptable where they would not be for a result.
 *
 * <p>Bounded like {@code ReturnedEmbeddingFields}' mapping cache: at {@link #CAPACITY} entries the table is cleared and
 * relearned rather than evicted piecemeal, which keeps the hot path to one map read. Keyed by index <i>name</i> rather
 * than {@code Index}: hits carry the name, and an index recreated under the same name is re-learned within a few pages.
 */
public final class ObservedSourceSizes {

    /** Weight of the newest page mean in the running estimate. */
    static final double EWMA_WEIGHT = 0.3;
    static final int CAPACITY = 4096;
    /** The shape key of a request that returns the whole {@code _source}. */
    static final String UNFILTERED = "*";

    private static final Map<String, Double> BYTES_PER_HIT = new ConcurrentHashMap<>();

    private ObservedSourceSizes() {}

    /**
     * Record what a response returned: for every index its hits came from, the mean serialized {@code _source} bytes of
     * the hits that carried one. A response whose hits carry no {@code _source} — the request had it off, or fetched
     * fields only — records nothing: there is nothing to learn about the {@code _source} volume from it. Cheap by
     * construction: one pass over at most {@code size} hits, one map write per index.
     */
    public static void record(final SearchSourceBuilder source, final SearchHits hits) {
        if (Objects.isNull(hits) || Objects.isNull(hits.getHits()) || hits.getHits().length == 0 || sourceRequested(source) == false) {
            return;
        }
        String shape = shapeKey(source);
        Map<String, long[]> perIndex = new HashMap<>();
        for (SearchHit hit : hits.getHits()) {
            if (hit.hasSource() == false || Objects.isNull(hit.getIndex())) {
                continue;
            }
            long[] sumAndCount = perIndex.computeIfAbsent(hit.getIndex(), index -> new long[2]);
            sumAndCount[0] += hit.getSourceRef().length();
            sumAndCount[1]++;
        }
        if (perIndex.isEmpty()) {
            return;
        }
        if (BYTES_PER_HIT.size() >= CAPACITY) {
            BYTES_PER_HIT.clear();
        }
        for (Map.Entry<String, long[]> entry : perIndex.entrySet()) {
            double pageMean = (double) entry.getValue()[0] / entry.getValue()[1];
            BYTES_PER_HIT.merge(key(entry.getKey(), shape), pageMean, (previous, latest) -> previous + EWMA_WEIGHT * (latest - previous));
        }
    }

    /**
     * The learned {@code _source} bytes one returned document of {@code index} carries under this request's {@code _source}
     * filter, or empty when no response of that shape has been observed yet.
     */
    public static OptionalLong bytesPerDocument(final String index, final SearchSourceBuilder source) {
        Double observed = BYTES_PER_HIT.get(key(index, shapeKey(source)));
        return Objects.isNull(observed) ? OptionalLong.empty() : OptionalLong.of(Math.round(observed));
    }

    /** Whether the request returns {@code _source} at all — unset means on, as core defaults it. */
    static boolean sourceRequested(final SearchSourceBuilder source) {
        FetchSourceContext fetchSource = Objects.isNull(source) ? null : source.fetchSource();
        return Objects.isNull(fetchSource) || fetchSource.fetchSource();
    }

    /**
     * The request's {@code _source} includes/excludes in one canonical string, so that {@code ["b","a"]} and
     * {@code ["a","b"]}, and an absent context and {@code _source: true}, share a key.
     */
    static String shapeKey(final SearchSourceBuilder source) {
        FetchSourceContext fetchSource = Objects.isNull(source) ? null : source.fetchSource();
        if (Objects.isNull(fetchSource)) {
            return UNFILTERED;
        }
        String[] includes = sorted(fetchSource.includes());
        String[] excludes = sorted(fetchSource.excludes());
        if (includes.length == 0 && excludes.length == 0) {
            return UNFILTERED;
        }
        return "i:" + String.join(",", includes) + "|e:" + String.join(",", excludes);
    }

    private static String[] sorted(final String[] patterns) {
        if (Objects.isNull(patterns) || patterns.length == 0) {
            return new String[0];
        }
        String[] copy = Arrays.copyOf(patterns, patterns.length);
        Arrays.sort(copy);
        return copy;
    }

    private static String key(final String index, final String shape) {
        return index + "\u0000" + shape;
    }

    /** Test hook: forget every observation. */
    @VisibleForTesting
    public static void clear() {
        BYTES_PER_HIT.clear();
    }

    /** Test hook: how many (index, shape) pairs have been observed. */
    static int size() {
        return BYTES_PER_HIT.size();
    }
}
