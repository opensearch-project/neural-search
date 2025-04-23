/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * KMeans++ clustering algorithm
 */
@AllArgsConstructor
public class KMeansPlusPlus implements Clustering {
    private final float alpha;
    private final int beta;
    private final SparseVectorReader reader;

    @Override
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException {
        int size = docFreqs.size();
        // generate beta unique random centers
        Random random = new Random();
        int num_cluster = Math.min(beta, size);
        int[] centers = random.ints(0, size).distinct().limit(num_cluster).toArray();
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
        }

        for (DocFreq docFreq : docFreqs) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            SparseVector docVector = reader.read(docFreq.getDocID());
            if (docVector == null) continue;
            for (int i = 0; i < num_cluster; i++) {
                SparseVector center = reader.read(centers[i]);
                float score = Float.MIN_VALUE;
                if (center != null) {
                    score = center.dotProduct(docVector);
                }
                if (score > maxScore) {
                    maxScore = score;
                    centerIdx = i;
                }
            }
            docAssignments.get(centerIdx).add(docFreq);
        }
        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < num_cluster; ++i) {
            if (docAssignments.get(i).isEmpty()) continue;
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            clusters.add(cluster);
        }
        return clusters;
    }
}
