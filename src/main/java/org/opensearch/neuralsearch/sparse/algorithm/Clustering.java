/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.neuralsearch.sparse.common.DocFreq;

import java.io.IOException;
import java.util.List;

/**
 * Interface for clustering algorithm
 */
public interface Clustering {
    List<DocumentCluster> cluster(List<DocFreq> docFreqs) throws IOException;
}
