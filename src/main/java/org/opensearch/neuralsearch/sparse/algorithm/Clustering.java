/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;

import java.io.IOException;
import java.util.List;

/**
 * Document clustering algorithm interface.
 */
public interface Clustering {
    /**
     * Clusters documents into a list of document clusters.
     *
     * @param docWeights documents to cluster (usually a posting list)
     * @return list of document clusters
     * @throws IOException if clustering fails
     */
    List<DocumentCluster> cluster(List<DocWeight> docWeights) throws IOException;
}
