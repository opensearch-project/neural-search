/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * Utility class for processing document postings in sparse neural search.
 *
 * <p>This class provides static methods for sorting, pruning, and summarizing
 * document postings. It supports operations like selecting top-K documents
 * by weight and generating cluster summaries from sparse vectors.
 *
 * <p>The class is designed to optimize posting lists for clustering algorithms
 * and improve search performance through intelligent document selection and
 * summarization techniques.
 *
 * @see DocWeight
 * @see DocumentCluster
 * @see SparseVector
 */
public class PostingsProcessor {

    /**
     * Selects the top-K documents from postings based on their weights.
     *
     * <p>Uses a priority queue to efficiently select the K documents with
     * the highest weights. If K is greater than or equal to the total number
     * of postings, returns all postings.
     *
     * @param postings the list of document postings to process
     * @param K the maximum number of documents to select
     * @return the top-K documents by weight, or all documents if K >= size
     */
    public static List<DocWeight> getTopK(List<DocWeight> postings, int K) {
        if (CollectionUtils.isEmpty(postings) || K == 0) {
            return Collections.emptyList();
        }
        if (K >= postings.size()) {
            return postings;
        }
        PriorityQueue<DocWeight> pq = new PriorityQueue<>(K, (o1, o2) -> ByteQuantizer.compareUnsignedByte(o1.getWeight(), o2.getWeight()));
        for (DocWeight docWeight : postings) {
            pq.add(docWeight);
            if (pq.size() > K) {
                pq.poll();
            }
        }
        return new ArrayList<>(pq);
    }

    /**
     * Generates a summary sparse vector for a document cluster.
     *
     * <p>Creates a cluster summary by aggregating token weights across all
     * documents in the cluster, taking the maximum weight for each token.
     * The summary is then pruned based on the specified ratio to keep only
     * the most significant tokens.
     *
     * @param cluster the document cluster to summarize
     * @param reader the sparse vector reader for accessing document vectors
     * @param summaryPruneRatio the ratio of total frequency to retain in summary (0-1)
     * @throws IOException if reading sparse vectors fails
     */
    public static void summarize(DocumentCluster cluster, SparseVectorReader reader, float summaryPruneRatio) throws IOException {
        Map<Integer, Integer> summary = new HashMap<>();
        DocWeightIterator iterator = cluster.getDisi();
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int docId = iterator.docID();
            SparseVector vector = reader.read(docId);
            if (vector != null) {
                IteratorWrapper<SparseVector.Item> vectorIterator = vector.iterator();
                while (vectorIterator.hasNext()) {
                    SparseVector.Item item = vectorIterator.next();
                    if (!summary.containsKey(item.getToken())) {
                        summary.put(item.getToken(), item.getIntWeight());
                    } else {
                        summary.put(item.getToken(), Math.max(summary.get(item.getToken()), item.getIntWeight()));
                    }
                }
            }
        }
        // convert summary to a SparseVector
        List<SparseVector.Item> items = summary.entrySet()
            .stream()
            .map(entry -> new SparseVector.Item(entry.getKey(), (byte) entry.getValue().intValue()))
            .sorted((o1, o2) -> ByteQuantizer.compareUnsignedByte(o2.getWeight(), o1.getWeight()))
            .collect(Collectors.toList());
        // count total freq of items
        double totalFreq = items.stream().mapToDouble(SparseVector.Item::getIntWeight).sum();
        int freqThreshold = (int) Math.floor(totalFreq * summaryPruneRatio);
        int freqSum = 0;
        int idx = 0;
        for (SparseVector.Item item : items) {
            ++idx;
            freqSum += item.getIntWeight();
            if (freqSum > freqThreshold) {
                break;
            }
        }
        items = items.subList(0, idx);
        cluster.setSummary(new SparseVector(items));
    }
}
