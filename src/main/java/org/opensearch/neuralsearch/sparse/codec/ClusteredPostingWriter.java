/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;

import java.util.List;

@FunctionalInterface
public interface ClusteredPostingWriter {
    void write(BytesRef term, List<DocumentCluster> clusters);
}
