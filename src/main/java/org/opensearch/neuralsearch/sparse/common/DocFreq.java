/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * DocFreq class to store docID and freq
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public final class DocFreq implements Comparable<DocFreq> {
    private final int docID;
    private final float freq;

    @Override
    public int compareTo(DocFreq o) {
        return Integer.compare(this.docID, o.docID);
    }
}
