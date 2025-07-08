/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;

import java.util.Set;

public interface ClusteredPostingReader {
    PostingClusters read(BytesRef term);

    Set<BytesRef> getTerms();

    long size();
}
