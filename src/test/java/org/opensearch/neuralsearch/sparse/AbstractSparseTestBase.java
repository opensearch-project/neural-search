/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AbstractSparseTestBase extends OpenSearchQueryTestCase {

    protected DocFreqIterator constructDocFreqIterator(Integer... docs) {
        return constructDocFreqIterator(Arrays.asList(docs), Arrays.asList(docs));
    }

    protected DocFreqIterator constructDocFreqIterator(List<Integer> docs, List<Integer> freqs) {
        return new DocFreqIterator() {
            int i = -1;

            @Override
            public byte freq() {
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

    protected List<DocFreq> preparePostings(int... docFreqs) {
        List<DocFreq> postings = new ArrayList<>();
        for (int i = 0; i < docFreqs.length; i += 2) {
            postings.add(new DocFreq(docFreqs[i], (byte) docFreqs[i + 1]));
        }
        return postings;
    }

    protected SparseVector createVector(int... docFreqs) {
        List<SparseVector.Item> items = new ArrayList<>();
        for (int i = 0; i < docFreqs.length; i += 2) {
            items.add(new SparseVector.Item(docFreqs[i], (byte) docFreqs[i + 1]));
        }
        return new SparseVector(items);
    }
}
