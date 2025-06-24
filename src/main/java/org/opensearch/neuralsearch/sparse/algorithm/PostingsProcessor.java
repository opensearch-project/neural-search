/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * This is a utility class for processing postings. It is used by the clustering algorithm to sort and prune postings.
 */
public class PostingsProcessor {

    public static List<DocFreq> getTopK(List<DocFreq> postings, int K) {
        if (K >= postings.size()) {
            return postings;
        }
        PriorityQueue<DocFreq> pq = new PriorityQueue<>(K, (o1, o2) -> ByteQuantizer.compareUnsignedByte(o1.getFreq(), o2.getFreq()));
        for (DocFreq docFreq : postings) {
            pq.add(docFreq);
            if (pq.size() > K) {
                pq.poll();
            }
        }
        return new ArrayList<>(pq);
    }

    public static void summarize(DocumentCluster cluster, SparseVectorReader reader, float summaryPruneRatio) throws IOException {
        Map<Integer, Integer> summary = new HashMap<>();
        DocFreqIterator iterator = cluster.getDisi();
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int docId = iterator.docID();
            SparseVector vector = reader.read(docId);
            if (vector != null) {
                IteratorWrapper<SparseVector.Item> vectorIterator = vector.iterator();
                while (vectorIterator.hasNext()) {
                    SparseVector.Item item = vectorIterator.next();
                    if (!summary.containsKey(item.getToken())) {
                        summary.put(item.getToken(), item.getIntFreq());
                    } else {
                        summary.put(item.getToken(), Math.max(summary.get(item.getToken()), item.getIntFreq()));
                    }
                }
            }
        }
        // convert summary to a SparseVector
        List<SparseVector.Item> items = summary.entrySet()
            .stream()
            .map(entry -> new SparseVector.Item(entry.getKey(), (byte) entry.getValue().intValue()))
            .sorted((o1, o2) -> ByteQuantizer.compareUnsignedByte(o2.getFreq(), o1.getFreq()))
            .collect(Collectors.toList());
        // count total freq of items
        double totalFreq = items.stream().mapToDouble(SparseVector.Item::getIntFreq).sum();
        int freqThreshold = (int) Math.floor(totalFreq * summaryPruneRatio);
        int freqSum = 0;
        int idx = 0;
        for (SparseVector.Item item : items) {
            ++idx;
            freqSum += item.getIntFreq();
            if (freqSum > freqThreshold) {
                break;
            }
        }
        items = items.subList(0, idx);
        cluster.setSummary(new SparseVector(items));
    }
}
