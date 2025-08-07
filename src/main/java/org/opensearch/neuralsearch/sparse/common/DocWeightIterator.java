/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * DocWeightIterator is a DocIdSetIterator that also provides the weight of the term in the document
 */
public abstract class DocWeightIterator extends DocIdSetIterator {
    public abstract byte weight();
}
