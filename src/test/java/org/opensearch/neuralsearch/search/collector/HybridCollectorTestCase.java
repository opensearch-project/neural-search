/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.LeafCollector;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Base class for HybridCollector test cases
 */
public class HybridCollectorTestCase extends OpenSearchQueryTestCase {
    /**
     * Index writer config that keeps insertion order on merge. newIndexWriterConfig() can pick
     * MockRandomMergePolicy, which may reverse doc order on forceMerge; LogMergePolicy merges
     * adjacent segments only. Not for tests that assert leaves().size() > 1; use
     * {@link #multiSegmentIndexWriterConfig()}.
     */
    protected static IndexWriterConfig orderPreservingIndexWriterConfig() {
        return newIndexWriterConfig().setMergePolicy(newLogMergePolicy());
    }

    /**
     * Index writer config that never merges, so each flush stays a separate segment. For tests
     * that assert leaves().size() > 1.
     */
    protected static IndexWriterConfig multiSegmentIndexWriterConfig() {
        return newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    }

    /**
     * Collect docs and scores for each sub-query scorer and add them to the leaf collector
     * @param scorer HybridSubQueryScorer object
     * @param scores1 List of scores for the first sub-query
     * @param leafCollector LeafCollector object
     * @param subQueryIndex Index of the sub-query
     * @param docsIds Array of document IDs
     * @throws IOException
     */
    void collectDocsAndScores(
        HybridSubQueryScorer scorer,
        List<Float> scores1,
        LeafCollector leafCollector,
        int subQueryIndex,
        int[] docsIds
    ) throws IOException {
        for (int i = 0; i < docsIds.length; i++) {
            scorer.getSubQueryScores()[subQueryIndex] = scores1.get(i);
            leafCollector.collect(docsIds[i]);
            scorer.resetScores();
        }
    }
}
