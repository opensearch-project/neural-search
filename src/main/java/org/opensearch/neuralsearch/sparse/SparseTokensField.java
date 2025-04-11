/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableFieldType;

public class SparseTokensField extends Field {

    public SparseTokensField(String name, byte[] value, IndexableFieldType type) {
        super(name, value, type);
    }
}
