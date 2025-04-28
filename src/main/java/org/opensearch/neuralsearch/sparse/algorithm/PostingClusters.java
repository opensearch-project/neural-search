/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.Getter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.util.List;

/**
 * This class represents the clusters of postings for a field
 */
@Getter
public class PostingClusters implements Accountable {
    private final List<DocumentCluster> clusters;
    private final int size;

    public PostingClusters(List<DocumentCluster> clusters) {
        this.clusters = clusters;
        if (clusters == null) {
            size = 0;
        } else {
            int count = 0;
            for (DocumentCluster cluster : clusters) {
                count += cluster.size();
            }
            size = count;
        }
    }

    public IteratorWrapper<DocumentCluster> iterator() {
        return new IteratorWrapper<DocumentCluster>(this.clusters.iterator());
    }

    @Override
    public long ramBytesUsed() {
        long ramUsed = RamUsageEstimator.shallowSizeOfInstance(PostingClusters.class);
        for (DocumentCluster cluster : clusters) {
            ramUsed += cluster.ramBytesUsed();
        }
        return ramUsed;
    }
}
