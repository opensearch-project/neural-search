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
 * Utility class for processing document postings.
 */
public class PostingsProcessingUtils {

    /**
     * Selects top-K documents by weight.
     *
     * @param postings document postings to process
     * @param K maximum number of documents to select
     * @return top-K documents by weight
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
     * Generates cluster summary sparse vector.
     * <p>
     * Construct summary vector using the max value from tokens of each vector in the cluster.
     * Then prune the summary vector by only keeping tokens with the largest weights which
     * takes summaryPruneRatio of the total weight.
     *
     * @param cluster document cluster to summarize
     * @param reader sparse vector reader
     * @param summaryPruneRatio summary weight sum ratio to retain (0-1)
     * @throws IOException if reading vectors fails
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
        // count total weight of items
        double totalWeight = items.stream().mapToDouble(SparseVector.Item::getIntWeight).sum();
        int weightThreshold = (int) Math.floor(totalWeight * summaryPruneRatio);
        int weightSum = 0;
        int idx = 0;
        for (SparseVector.Item item : items) {
            ++idx;
            weightSum += item.getIntWeight();
            if (weightSum > weightThreshold) {
                break;
            }
        }
        items = items.subList(0, idx);
        cluster.setSummary(new SparseVector(items));
    }
}
