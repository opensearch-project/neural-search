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
 * Represents a cluster of documents with their associated weights and a summary vector.
 * Used in sparse vector search to group similar documents for efficient retrieval.
 * Implements memory accounting for RAM usage tracking.
 */
@Getter
@Setter
@EqualsAndHashCode
public class DocumentCluster implements Accountable {
    /** Summary sparse vector representing the cluster. */
    private SparseVector summary;
    /** Document IDs in this cluster, sorted in ascending order. */
    private final int[] docIds;
    /** Weights corresponding to each document ID. */
    private final byte[] weights;
    /** Flag indicating if documents in this cluster should always be examined. */
    private boolean shouldNotSkip;

    /**
     * Creates a document cluster with the given summary vector and documents.
     * Documents are sorted by ID for efficient access.
     *
     * @param summary the sparse vector summarizing this cluster
     * @param docs the list of documents with their weights
     * @param shouldNotSkip whether this cluster should always be examined
     */
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

    /**
     * Returns the number of documents in this cluster.
     *
     * @return the cluster size, or 0 if no documents
     */
    public int size() {
        return docIds == null ? 0 : docIds.length;
    }

    /**
     * Returns an iterator over the documents in this cluster.
     *
     * @return iterator of DocWeight objects combining document IDs and weights
     */
    public Iterator<DocWeight> iterator() {
        return new CombinedIterator<>(
            new ArrayIterator.IntArrayIterator(docIds),
            new ArrayIterator.ByteArrayIterator(weights),
            DocWeight::new
        );
    }

    /**
     * Returns a DocWeightIterator for efficient document iteration.
     *
     * @return a DocWeightIterator implementation for this cluster
     */
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

    /**
     * Calculates the total RAM usage of this document cluster.
     *
     * @return the total bytes used in RAM
     */
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

    /**
     * Returns child resources for memory accounting.
     *
     * @return collection of child Accountable objects
     */
    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> children = new ArrayList<>();
        if (summary != null) {
            children.add(summary);
        }
        return Collections.unmodifiableList(children);
    }
}
