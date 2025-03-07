/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.search.grouping.SearchGroup;

public class HybridCollectedSearchGroup<T> extends SearchGroup<T> {
    int topDoc;
    int comparatorSlot;

    public HybridCollectedSearchGroup() {}
}
