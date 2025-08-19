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
 * Preprocesses and clusters document postings.
 */
public class PostingClustering {

    private final static int MINIMAL_DOC_SIZE_TO_CLUSTER = 10;
    private final int nPostings;
    private final Clustering clustering;

    /**
     * Constructs a PostingClustering instance.
     *
     * @param nPostings maximum number of postings to consider
     * @param clustering clustering algorithm
     */
    public PostingClustering(int nPostings, Clustering clustering) {
        this.nPostings = nPostings;
        this.clustering = clustering;
    }

    /**
     * Preprocess postings by selecting top-K documents by weight.
     *
     * @param postings input postings
     * @return top-K postings
     */
    private List<DocWeight> preprocess(List<DocWeight> postings) {
        return PostingsProcessor.getTopK(postings, nPostings);
    }

    /**
     * Clusters document postings using the provided {@link Clustering} algorithm.
     *
     * @param postings document postings to cluster
     * @return list of document clusters
     * @throws IOException if clustering fails
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
