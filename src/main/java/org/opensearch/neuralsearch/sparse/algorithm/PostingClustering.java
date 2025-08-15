/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.commons.collections4.CollectionUtils;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Preprocesses and clusters document postings for sparse neural search optimization.
 *
 * <p>This class combines preprocessing and clustering operations to optimize
 * document postings for neural search. It first applies preprocessing to select
 * the top-K postings, then applies clustering algorithms to group similar documents.
 *
 * <p>The class handles edge cases such as empty postings and small document sets
 * that don't meet the minimum clustering threshold.
 *
 * @see Clustering
 * @see PostingsProcessor
 * @see DocumentCluster
 */
public class PostingClustering {

    private final static int MINIMAL_DOC_SIZE_TO_CLUSTER = 10;
    private final int nPostings;
    private final Clustering clustering;

    /**
     * Constructs a PostingClustering instance.
     *
     * @param nPostings the maximum number of postings to consider after preprocessing
     * @param clustering the clustering algorithm to use for grouping documents
     */
    public PostingClustering(int nPostings, Clustering clustering) {
        this.nPostings = nPostings;
        this.clustering = clustering;
    }

    /**
     * Preprocesses postings by selecting the top-K documents by weight.
     *
     * @param postings the input postings to preprocess
     * @return the top-K postings based on document weights
     */
    private List<DocWeight> preprocess(List<DocWeight> postings) {
        return PostingsProcessor.getTopK(postings, nPostings);
    }

    /**
     * Clusters document postings after preprocessing.
     *
     * <p>This method first preprocesses the postings to select top-K documents,
     * then applies clustering if the document count meets the minimum threshold.
     * For small document sets, returns a single cluster containing all documents.
     *
     * @param postings the document postings to cluster
     * @return a list of document clusters, empty if no postings provided
     * @throws IOException if clustering operation fails
     */
    public List<DocumentCluster> cluster(List<DocWeight> postings) throws IOException {
        if (CollectionUtils.isEmpty(postings)) {
            return Collections.emptyList();
        }
        List<DocWeight> postingsCopy = new ArrayList<>(postings);
        List<DocWeight> preprocessed = preprocess(postingsCopy);
        if (preprocessed.isEmpty()) {
            return Collections.emptyList();
        }
        if (preprocessed.size() < MINIMAL_DOC_SIZE_TO_CLUSTER) {
            return Collections.singletonList(new DocumentCluster(null, preprocessed, true));
        }
        return clustering.cluster(preprocessed);
    }
}
