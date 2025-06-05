/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.ArrayIterator;
import org.opensearch.neuralsearch.sparse.common.CombinedIterator;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Class to represent a document cluster
 */
@Getter
@Setter
@EqualsAndHashCode
public class DocumentCluster implements Accountable {
    private SparseVector summary;
    // private final List<DocFreq> docs;
    private final int[] docIds;
    private final byte[] freqs;
    // if true, docs in this cluster should always be examined
    private boolean shouldNotSkip;

    public DocumentCluster(SparseVector summary, List<DocFreq> docs, boolean shouldNotSkip) {
        this.summary = summary;
        List<DocFreq> docsCopy = new ArrayList<>(docs);
        docsCopy.sort((o1, o2) -> ByteQuantizer.compareUnsignedByte(o1.getFreq(), o2.getFreq()));
        int size = docsCopy.size();
        this.docIds = new int[size];
        this.freqs = new byte[size];
        for (int i = 0; i < size; i++) {
            DocFreq docFreq = docsCopy.get(i);
            this.docIds[i] = docFreq.getDocID();
            this.freqs[i] = docFreq.getFreq();
        }
        this.shouldNotSkip = shouldNotSkip;
    }

    public int size() {
        return docIds == null ? 0 : docIds.length;
    }

    public Iterator<DocFreq> iterator() {
        return new CombinedIterator<>(new ArrayIterator.IntArrayIterator(docIds), new ArrayIterator.ByteArrayIterator(freqs), DocFreq::new);
    }

    public DocFreqIterator getDisi() {
        return new DocFreqIterator() {
            final IteratorWrapper<DocFreq> wrapper = new IteratorWrapper<>(iterator());

            @Override
            public byte freq() {
                return wrapper.getCurrent().getFreq();
            }

            @Override
            public int docID() {
                if (wrapper.getCurrent() == null) {
                    return -1;
                }
                return wrapper.getCurrent().getDocID();
            }

            @Override
            public int nextDoc() {
                if (wrapper.hasNext()) {
                    return wrapper.next().getDocID();
                }
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public long ramBytesUsed() {
        long sizeInBytes = 0;
        sizeInBytes += RamUsageEstimator.shallowSizeOfInstance(DocumentCluster.class);
        if (docIds != null) {
            sizeInBytes += RamUsageEstimator.sizeOf(docIds);
            sizeInBytes += RamUsageEstimator.sizeOf(freqs);
        }
        return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> children = new ArrayList<>();
        if (summary != null) {
            children.add(summary);
        }
        return Collections.unmodifiableList(children);
    }
}
