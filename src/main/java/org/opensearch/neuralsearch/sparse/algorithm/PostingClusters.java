/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.Getter;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.util.List;

/**
 * This class represents the clusters of postings for a field
 */
@Getter
public class PostingClusters {
    private final List<DocumentCluster> clusters;

    public PostingClusters(List<DocumentCluster> clusters) {
        this.clusters = clusters;
    }

    public IteratorWrapper iterator() {
        return new IteratorWrapper(this.clusters.iterator());
    }
}
