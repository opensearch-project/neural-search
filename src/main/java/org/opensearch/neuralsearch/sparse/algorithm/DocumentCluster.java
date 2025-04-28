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
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class to represent a document cluster
 */
@Getter
@Setter
@EqualsAndHashCode
public class DocumentCluster implements Accountable {
    private SparseVector summary;
    private final List<DocFreq> docs;
    // if true, docs in this cluster should always be examined
    private boolean shouldNotSkip;

    public DocumentCluster(SparseVector summary, List<DocFreq> docs, boolean shouldNotSkip) {
        this.summary = summary;
        List<DocFreq> docsCopy = new ArrayList<>(docs);
        docsCopy.sort((a, b) -> Integer.compare(a.getDocID(), b.getDocID()));
        this.docs = Collections.unmodifiableList(docsCopy);
        this.shouldNotSkip = shouldNotSkip;
    }

    public int size() {
        return docs == null ? 0 : docs.size();
    }

    public DocFreqIterator getDisi() {
        return new DocFreqIterator() {
            final IteratorWrapper<DocFreq> wrapper = new IteratorWrapper<>(docs.iterator());

            @Override
            public float freq() {
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
        return RamUsageEstimator.shallowSizeOfInstance(DocumentCluster.class) + docs.size() * RamUsageEstimator.shallowSizeOfInstance(
            DocFreq.class
        );
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
