/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.ArrayIterator;
import org.opensearch.neuralsearch.sparse.common.CombinedIterator;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
    private final int[] docIds;
    private final byte[] weights;
    // if true, docs in this cluster should always be examined
    private boolean shouldNotSkip;

    public DocumentCluster(SparseVector summary, List<DocWeight> docs, boolean shouldNotSkip) {
        this.summary = summary;
        List<DocWeight> docsCopy = new ArrayList<>(docs);
        docsCopy.sort(Comparator.comparingInt(DocWeight::getDocID));
        int size = docsCopy.size();
        this.docIds = new int[size];
        this.weights = new byte[size];
        for (int i = 0; i < size; i++) {
            DocWeight docWeight = docsCopy.get(i);
            this.docIds[i] = docWeight.getDocID();
            this.weights[i] = docWeight.getWeight();
        }
        this.shouldNotSkip = shouldNotSkip;
    }

    public int size() {
        return docIds == null ? 0 : docIds.length;
    }

    public Iterator<DocWeight> iterator() {
        return new CombinedIterator<>(
            new ArrayIterator.IntArrayIterator(docIds),
            new ArrayIterator.ByteArrayIterator(weights),
            DocWeight::new
        );
    }

    public DocWeightIterator getDisi() {
        return new DocWeightIterator() {
            final IteratorWrapper<DocWeight> wrapper = new IteratorWrapper<>(iterator());

            @Override
            public byte weight() {
                return wrapper.getCurrent().getWeight();
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
            sizeInBytes += RamUsageEstimator.sizeOf(weights);
        }
        if (summary != null) {
            sizeInBytes += summary.ramBytesUsed();
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
