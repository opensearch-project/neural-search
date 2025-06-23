/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.neuralsearch.sparse.common.DocFreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class preprocesses postings then clusters them using the given clustering algorithm.
 */
public class PostingClustering {

    private final static int MINIMAL_DOC_SIZE_TO_CLUSTER = 10;
    private final int lambda;
    private final Clustering clustering;

    public PostingClustering(int lambda, Clustering clustering) {
        this.lambda = lambda;
        this.clustering = clustering;
    }

    private List<DocFreq> preprocess(List<DocFreq> postings) {
        return PostingsProcessor.getTopK(postings, lambda);
    }

    public List<DocumentCluster> cluster(List<DocFreq> postings) throws IOException {
        List<DocFreq> postingsCopy = new ArrayList<>(postings);
        List<DocFreq> preprocessed = preprocess(postingsCopy);
        if (preprocessed.isEmpty()) {
            return new ArrayList<>();
        }
        if (preprocessed.size() < MINIMAL_DOC_SIZE_TO_CLUSTER) {
            return Collections.singletonList(new DocumentCluster(null, preprocessed, true));
        }
        return clustering.cluster(preprocessed);
    }
}
