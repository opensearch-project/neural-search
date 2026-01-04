/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A LeafCollector for phase one of two-phase sparse vector search.
 * Collects top-scoring documents using a min-heap, encoding doc IDs and scores into longs for efficient storage.
 */
public class PhaseOneLeafCollector implements LeafCollector {
    private Scorable scorer;
    private final LongHeap resultHeap;

    /**
     * @param resultSize maximum number of top results to retain
     */
    public PhaseOneLeafCollector(int resultSize) {
        this.resultHeap = new LongHeap(resultSize);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
        float score = this.scorer.score();
        long encoded = encode(doc, score);
        resultHeap.insertWithOverflow(encoded);
    }

    /**
     * Returns collected results as an iterator sorted by doc ID ascending.
     */
    public ResultsDocValueIterator<Float> getPhaseOneResults() {
        List<Pair<Integer, Float>> results = new ArrayList<>(resultHeap.size());
        for (int i = 1; i <= resultHeap.size(); i++) {
            long encoded = resultHeap.get(i);
            results.add(Pair.of(decodeDocId(encoded), decodeScore(encoded)));
        }
        results.sort(Comparator.comparingInt(Pair::getLeft));
        return new ResultsDocValueIterator<>(results);
    }

    private static long encode(int docId, float score) {
        return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (Integer.MAX_VALUE - docId);
    }

    private static int decodeDocId(long encoded) {
        return Integer.MAX_VALUE - (int) encoded;
    }

    private static float decodeScore(long encoded) {
        return NumericUtils.sortableIntToFloat((int) (encoded >>> 32));
    }
}
