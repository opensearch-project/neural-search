/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * DocFreqIterator is a DocIdSetIterator that also provides the frequency of the term in the document
 */
public abstract class DocFreqIterator extends DocIdSetIterator {
    public abstract float freq();
}
