/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;

/**
 * Lucene field for sparse token storage.
 */
public class SparseTokensField extends Field {
    public static final String SPARSE_FIELD = "sparse_tokens_field";

    /**
     * Creates sparse tokens field with byte array value.
     */
    public SparseTokensField(String name, byte[] value, IndexableFieldType type) {
        super(name, value, type);
    }

    /**
     * Checks if field is a sparse field.
     */
    public static boolean isSparseField(FieldInfo field) {
        if (field == null) {
            return false;
        }
        return field.attributes().containsKey(SPARSE_FIELD);
    }
}
