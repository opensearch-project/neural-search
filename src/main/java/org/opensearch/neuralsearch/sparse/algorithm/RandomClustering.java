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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Random clustering algorithm
 */
@AllArgsConstructor
public class RandomClustering implements Clustering {

    private final static int MINIMAL_CLUSTER_DOC_SIZE = 3;

    private final int lambda;
    private final float alpha;
    private final int beta;
    private final SparseVectorReader reader;

    /**
     * Assigns documents to clusters based on similarity.
     *
     * @param documents The list of documents to assign
     * @param docAssignments The list of document assignments for each cluster
     * @param denseCentroids The list of cluster centroids
     * @param clusterIds The list of cluster IDs to consider
     */
    private void assignDocumentsToCluster(
        List<DocFreq> documents,
        List<List<DocFreq>> docAssignments,
        List<float[]> denseCentroids,
        List<Integer> clusterIds
    ) {

        for (DocFreq docFreq : documents) {
            SparseVector docVector = reader.read(docFreq.getDocID());
            if (docVector == null) {
                continue;
            }

            int bestCluster = 0;
            float maxScore = Float.MIN_VALUE;

            for (int clusterId : clusterIds) {
                float[] center = denseCentroids.get(clusterId);
                if (center != null) {
                    float score = docVector.dotProduct(center);
                    if (score > maxScore) {
                        maxScore = score;
                        bestCluster = clusterId;
                    }
                }
            }

            docAssignments.get(bestCluster).add(docFreq);
        }
    }

    @Override
    public List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException {
        if (beta == 1) {
            DocumentCluster cluster = new DocumentCluster(null, docFreqs, true);
            return List.of(cluster);
        }
        int size = docFreqs.size();

        // Adjust num_cluster according to posting length
        int num_cluster = (int) Math.ceil((double) (size * beta) / lambda);

        // Generate beta unique random centers
        Random random = new Random();
        int[] centers = random.ints(0, size).distinct().limit(num_cluster).toArray();

        // Initialize centroids
        List<List<DocFreq>> docAssignments = new ArrayList<>(num_cluster);
        List<float[]> denseCentroids = new ArrayList<>();
        for (int i = 0; i < num_cluster; i++) {
            docAssignments.add(new ArrayList<>());
            SparseVector center = reader.read(docFreqs.get(centers[i]).getDocID());
            if (center == null) {
                denseCentroids.add(null);
            } else {
                denseCentroids.add(center.toDenseVector());
            }
        }

        // Create a list of all cluster indices
        List<Integer> allClusterIds = IntStream.range(0, num_cluster).boxed().collect(Collectors.toList());

        // Assign documents to clusters
        assignDocumentsToCluster(docFreqs, docAssignments, denseCentroids, allClusterIds);

        // Identify valid clusters
        List<Integer> validClusterIds = IntStream.range(0, num_cluster)
            .filter(i -> docAssignments.get(i).size() >= MINIMAL_CLUSTER_DOC_SIZE)
            .boxed()
            .collect(Collectors.toList());

        // Only proceed with reassignment if we have valid clusters to reassign to
        if (!validClusterIds.isEmpty()) {
            // Identify small clusters and collect their documents for reassignment using streams
            List<DocFreq> docsToReassign = IntStream.range(0, num_cluster)
                .filter(i -> docAssignments.get(i).size() < MINIMAL_CLUSTER_DOC_SIZE)
                .mapToObj(i -> {
                    List<DocFreq> docs = new ArrayList<>(docAssignments.get(i));
                    docAssignments.get(i).clear();
                    return docs;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

            // Reassign documents from small clusters
            assignDocumentsToCluster(docsToReassign, docAssignments, denseCentroids, validClusterIds);
        }

        List<DocumentCluster> clusters = new ArrayList<>();
        for (int i = 0; i < num_cluster; ++i) {
            if (docAssignments.get(i).isEmpty()) {
                continue;
            }
            DocumentCluster cluster = new DocumentCluster(null, docAssignments.get(i), false);
            PostingsProcessor.summarize(cluster, this.reader, this.alpha);
            clusters.add(cluster);
        }
        return clusters;
    }
}
