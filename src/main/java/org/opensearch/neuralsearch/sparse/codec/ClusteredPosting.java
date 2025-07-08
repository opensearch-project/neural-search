/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

public interface ClusteredPosting {
    ClusteredPostingReader getReader();  // covariant return type

    ClusteredPostingWriter getWriter();  // covariant return type
}
