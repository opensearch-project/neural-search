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
        int[] centers = random.ints(0, size).distinct().limit(beta).toArray();
        List<List<DocFreq>> docAssignments = new ArrayList<>(beta);
        for (int i = 0; i < beta; i++) {
            docAssignments.add(new ArrayList<>());
        }

        for (DocFreq docFreq : docFreqs) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            for (int i = 0; i < beta; i++) {
                SparseVector center = reader.read(centers[i]);
                SparseVector docVector = reader.read(docFreq.getDocID());
                float score = Float.MIN_VALUE;
                if (center != null && docVector != null) {
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
        for (int i = 0; i < beta; ++i) {
            if (docAssignments.get(i).isEmpty()) continue;
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            clusters.add(cluster);
        }
        return clusters;
    }
}
