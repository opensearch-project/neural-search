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
 * Interface for document clustering algorithms used in sparse neural search.
 *
 * <p>This interface defines the contract for clustering algorithms that group
 * documents based on their weight vectors. Implementations should provide
 * efficient clustering methods suitable for neural search applications.
 *
 * <p>Clustering is used to organize documents into groups with similar
 * characteristics, which can improve search performance and relevance
 * in neural search scenarios.
 *
 * @see DocWeight
 * @see DocumentCluster
 */
public interface Clustering {
    /**
     * Clusters documents based on their weight vectors.
     *
     * <p>This method takes a list of document weights and groups them into
     * clusters based on similarity or other clustering criteria defined by
     * the implementation.
     *
     * @param docWeights the list of document weights to cluster. Must not be null
     *                   or empty. Each DocWeight represents a document with its
     *                   associated weight vector.
     * @return a list of DocumentCluster objects, each containing a group of
     *         related documents. The returned list will not be null or empty.
     * @throws IOException if an I/O error occurs during the clustering process,
     *                     such as when reading model data or writing intermediate
     *                     results
     */
    List<DocumentCluster> cluster(List<DocWeight> docWeights) throws IOException;
}
