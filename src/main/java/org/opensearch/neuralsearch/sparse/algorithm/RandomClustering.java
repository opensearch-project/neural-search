/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Random clustering algorithm
 */
@AllArgsConstructor
public class RandomClustering implements Clustering {

    private final int lambda;
    private final float alpha;
    private final int beta;
    @NonNull
    private final SparseVectorReader reader;

    @Override
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException {
        if (docFreqs.isEmpty()) {
            return List.of();
        }
        if (beta == 1) {
            DocumentCluster cluster = new DocumentCluster(null, docFreqs, true);
            return List.of(cluster);
        }
        int size = docFreqs.size();
        // generate beta unique random centers
        Random random = new Random();
        int num_cluster = (int) Math.ceil((double) (size * beta) / lambda);
        // Ensure num_cluster doesn't exceed the available document count
        num_cluster = Math.min(num_cluster, size);
        int[] centers = random.ints(0, size).distinct().limit(num_cluster).toArray();
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        List<SparseVector> sparseVectors = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docFreqs.get(centers[i]).getDocID());
            sparseVectors.add(center);
        }

        for (DocFreq docFreq : docFreqs) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            SparseVector docVector = reader.read(docFreq.getDocID());
            if (docVector == null) {
                continue;
            }
            byte[] denseDocVector = docVector.toDenseVector();
            for (int i = 0; i < num_cluster; i++) {
                float score = Float.MIN_VALUE;
                SparseVector center = sparseVectors.get(i);
                if (center != null) {
                    score = center.dotProduct(denseDocVector);
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
