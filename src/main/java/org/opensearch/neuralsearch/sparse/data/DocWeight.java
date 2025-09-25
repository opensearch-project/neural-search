/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

/**
 * DocWeight class to store docID and weight
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public final class DocWeight implements Comparable<DocWeight> {
    private final int docID;
    private final byte weight;

    @Override
    public int compareTo(DocWeight o) {
        return Integer.compare(this.docID, o.docID);
    }

    public int getIntWeight() {
        return ByteQuantizationUtil.getUnsignedByte(weight);
    }
}
