/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

public interface SparseAlgorithm {
    ValidationException validateMethod(SparseMethodContext sparseMethodContext);
}
