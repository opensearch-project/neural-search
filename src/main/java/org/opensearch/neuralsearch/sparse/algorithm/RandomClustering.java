/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.opensearch.common.Randomness;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Random clustering algorithm for SEISMIC.
 */
@AllArgsConstructor
public class RandomClustering implements Clustering {

    private final float summaryPruneRatio;
    private final float clusterRatio;
    @NonNull
    private final SparseVectorReader reader;

    /**
     * Clusters documents using random centers and dot product similarity.
     *
     * @param docWeights documents to cluster
     * @return list of document clusters
     * @throws IOException if reading vectors fails
     */
    @Override
    public List<DocumentCluster> cluster(List<DocWeight> docWeights) throws IOException {
        if (CollectionUtils.isEmpty(docWeights)) {
            return Collections.emptyList();
        }
        if (clusterRatio == 0) {
            DocumentCluster cluster = new DocumentCluster(null, docWeights, true);
            return List.of(cluster);
        }
        int size = docWeights.size();
        // generate beta unique random centers
        int numCluster = Math.min(size, Math.max(1, (int) Math.ceil(size * clusterRatio)));
        int[] centers = Randomness.get().ints(0, size).distinct().limit(numCluster).toArray();
        List<List<DocWeight>> docAssignments = new ArrayList<>(numCluster);
        List<SparseVector> sparseVectors = new ArrayList<>();
        for (int i = 0; i < numCluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docWeights.get(centers[i]).getDocID());
            sparseVectors.add(center);
        }

        for (DocWeight docWeight : docWeights) {
            int centerIdx = 0;
            float maxScore = Float.MIN_VALUE;
            SparseVector docVector = reader.read(docWeight.getDocID());
            if (docVector == null) {
                continue;
            }
            byte[] denseDocVector = docVector.toDenseVector();
            for (int i = 0; i < numCluster; i++) {
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
            docAssignments.get(centerIdx).add(docWeight);
        }
        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < numCluster; ++i) {
            if (docAssignments.get(i).isEmpty()) continue;
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.summaryPruneRatio);
            clusters.add(cluster);
        }
        return clusters;
    }
}
