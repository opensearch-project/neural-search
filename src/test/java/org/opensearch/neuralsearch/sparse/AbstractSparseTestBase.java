/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.cache.CircuitBreakerManager;
import org.opensearch.core.common.breaker.CircuitBreaker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class AbstractSparseTestBase extends OpenSearchQueryTestCase {

    protected CircuitBreaker mockedCircuitBreaker = mock(CircuitBreaker.class);

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        CircuitBreakerManager.setCircuitBreaker(mockedCircuitBreaker);
    }

    protected DocWeightIterator constructDocWeightIterator(Integer... docs) {
        return constructDocWeightIterator(Arrays.asList(docs), Arrays.asList(docs));
    }

    protected DocWeightIterator constructDocWeightIterator(List<Integer> docs, List<Integer> freqs) {
        return new DocWeightIterator() {
            int i = -1;

            @Override
            public byte weight() {
                return (byte) (freqs.get(i) & 0xff);
            }

            @Override
            public int nextDoc() {
                if (i + 1 == docs.size()) {
                    return NO_MORE_DOCS;
                } else {
                    return docs.get(++i);
                }
            }

            @Override
            public int docID() {
                return i < 0 ? -1 : i == docs.size() ? NO_MORE_DOCS : docs.get(i);
            }

            @Override
            public long cost() {
                return docs.size();
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }
        };
    }

    protected List<DocWeight> preparePostings(int... docWeights) {
        List<DocWeight> postings = new ArrayList<>();
        for (int i = 0; i < docWeights.length; i += 2) {
            postings.add(new DocWeight(docWeights[i], (byte) docWeights[i + 1]));
        }
        return postings;
    }

    protected SparseVector createVector(int... docWeights) {
        List<SparseVector.Item> items = new ArrayList<>();
        for (int i = 0; i < docWeights.length; i += 2) {
            items.add(new SparseVector.Item(docWeights[i], (byte) docWeights[i + 1]));
        }
        return new SparseVector(items);
    }

    protected DocumentCluster prepareCluster(int summaryDP, boolean shouldNotSkip, byte[] queryDenseVector) {
        // Mock document cluster
        DocumentCluster cluster = mock(DocumentCluster.class);
        SparseVector clusterSummary = mock(SparseVector.class);
        when(cluster.getSummary()).thenReturn(clusterSummary);
        when(clusterSummary.dotProduct(queryDenseVector)).thenReturn(summaryDP);
        when(cluster.isShouldNotSkip()).thenReturn(shouldNotSkip);
        return cluster;
    }

    protected void prepareVectors(SparseVectorReader reader, byte[] queryDenseVector, int... arguments) throws IOException {
        for (int i = 0; i < arguments.length; i += 2) {
            prepareVector(arguments[i], arguments[i + 1], reader, queryDenseVector);
        }
    }

    protected SparseVector prepareVector(int docId, int dpScore, SparseVectorReader reader, byte[] queryDenseVector) throws IOException {
        SparseVector docVector = mock(SparseVector.class);
        when(reader.read(docId)).thenReturn(docVector);
        when(docVector.dotProduct(queryDenseVector)).thenReturn(dpScore);
        return docVector;
    }

    protected void prepareClusterAndItsDocs(SparseVectorReader reader, byte[] queryDenseVector, DocumentCluster cluster, int... docScores)
        throws IOException {
        prepareVectors(reader, queryDenseVector, docScores);
        List<Integer> docs = new ArrayList<>();
        for (int i = 0; i < docScores.length; i += 2) {
            docs.add(docScores[i]);
        }
        // Mock DocWeightIterator with two docs - one with vector and one without
        DocWeightIterator docIterator = constructDocWeightIterator(docs, docs);
        when(cluster.getDisi()).thenReturn(docIterator);
    }

    protected List<DocumentCluster> prepareClusterList() {
        List<DocumentCluster> clusters = new ArrayList<>();
        SparseVector documentSummary1 = createVector(1, 10, 2, 20);
        SparseVector documentSummary2 = createVector(1, 1, 2, 2);

        List<DocWeight> docWeights1 = new ArrayList<>();
        docWeights1.add(new DocWeight(0, (byte) 1));

        List<DocWeight> docWeights2 = new ArrayList<>();
        docWeights1.add(new DocWeight(1, (byte) 2));

        clusters.add(new DocumentCluster(documentSummary1, docWeights1, false));
        clusters.add(new DocumentCluster(documentSummary2, docWeights2, false));

        return clusters;
    }

    protected PostingClusters preparePostingClusters() {
        return new PostingClusters(prepareClusterList());
    }

    protected Map<String, String> prepareAttributes(boolean sparse, int threshold, float ratio, int posting, float summary) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SPARSE_FIELD, String.valueOf(sparse));
        attributes.put(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(threshold));
        attributes.put(CLUSTER_RATIO_FIELD, String.valueOf(ratio));
        attributes.put(N_POSTINGS_FIELD, String.valueOf(posting));
        attributes.put(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(summary));
        return attributes;
    }
}
