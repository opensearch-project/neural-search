/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;

public class SparseTokensField extends Field {
    public static final String SPARSE_FIELD = "sparse_tokens_field";

    public SparseTokensField(String name, byte[] value, IndexableFieldType type) {
        super(name, value, type);
    }

    public static boolean isSparseField(FieldInfo field) {
        return field.attributes().containsKey(SPARSE_FIELD);
    }
}
