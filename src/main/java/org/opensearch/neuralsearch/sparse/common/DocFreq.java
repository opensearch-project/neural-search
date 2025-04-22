/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DocFreq class to store docID and freq
 */
@AllArgsConstructor
@Getter
public final class DocFreq implements Comparable<DocFreq> {
    private final int docID;
    private final float freq;

    @Override
    public int hashCode() {
        return Integer.hashCode(docID) + Float.hashCode(freq);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        final DocFreq other = (DocFreq) obj;
        return this.docID == other.docID && this.freq == other.freq;
    }

    @Override
    public int compareTo(DocFreq o) {
        return Integer.compare(this.docID, o.docID);
    }
}
