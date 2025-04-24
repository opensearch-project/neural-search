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
    public static List<DocFreq> sortByFreq(List<DocFreq> postings) {
        postings.sort((o1, o2) -> Float.compare(o2.getFreq(), o1.getFreq()));
        return postings;
    }

    public static List<DocFreq> pruneBySize(List<DocFreq> postings, int size) {
        return postings.subList(0, Math.min(postings.size(), size));
    }

    public static List<DocFreq> getTopK(List<DocFreq> postings, int K) {
        PriorityQueue<DocFreq> pq = new PriorityQueue<>(K, (o1, o2) -> Float.compare(o2.getFreq(), o1.getFreq()));
        for (DocFreq docFreq : postings) {
            pq.add(docFreq);
            if (pq.size() > K) {
                pq.poll();
            }
        }
        return new ArrayList<>(pq);
    }

    public static void summarize(DocumentCluster cluster, SparseVectorReader reader, float alpha) throws IOException {
        Map<Integer, Float> summary = new HashMap<>();
        DocFreqIterator iterator = cluster.getDisi();
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int docId = iterator.docID();
            SparseVector vector = reader.read(docId);
            if (vector != null) {
                IteratorWrapper<SparseVector.Item> vectorIterator = vector.iterator();
                while (vectorIterator.hasNext()) {
                    SparseVector.Item item = vectorIterator.next();
                    if (!summary.containsKey(item.getToken())) {
                        summary.put(item.getToken(), item.getFreq());
                    } else {
                        summary.put(item.getToken(), summary.get(item.getToken()) + item.getFreq());
                    }
                }
            }
        }
        // convert summary to a SparseVector
        List<SparseVector.Item> items = summary.entrySet()
            .stream()
            .map(entry -> new SparseVector.Item(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
        ;
        items.sort((o1, o2) -> o2.getToken() - o1.getToken());
        // count total freq of items
        double totalFreq = items.stream().mapToDouble(SparseVector.Item::getFreq).sum();
        double freqThreshold = totalFreq * alpha;
        double freqSum = 0.0;
        int idx = 0;
        for (SparseVector.Item item : items) {
            ++idx;
            freqSum += item.getFreq();
            if (freqSum > freqThreshold) {
                break;
            }
        }
        items = items.subList(0, idx);
        cluster.setSummary(new SparseVector(items));
    }
}
