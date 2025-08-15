/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;

/**
 * Custom Lucene field implementation for storing sparse token data.
 *
 * <p>This field extends Lucene's Field class to provide specialized storage
 * for sparse token representations in neural search. It stores binary data
 * representing sparse vectors with their associated tokens and weights.
 *
 * <p>The field includes utility methods for identifying sparse fields within
 * Lucene's field metadata, enabling proper handling during indexing and
 * search operations.
 *
 * <p>Key features:
 * <ul>
 *   <li>Binary storage of sparse token data</li>
 *   <li>Field identification through metadata attributes</li>
 *   <li>Integration with Lucene's field system</li>
 * </ul>
 *
 * @see Field
 * @see IndexableFieldType
 * @see FieldInfo
 */
public class SparseTokensField extends Field {
    public static final String SPARSE_FIELD = "sparse_tokens_field";

    /**
     * Constructs a SparseTokensField with binary data.
     *
     * @param name the field name
     * @param value the binary data representing sparse tokens
     * @param type the indexable field type configuration
     */
    public SparseTokensField(String name, byte[] value, IndexableFieldType type) {
        super(name, value, type);
    }

    /**
     * Checks if a field is a sparse tokens field.
     *
     * <p>Determines if the given FieldInfo represents a sparse tokens field
     * by checking for the presence of the sparse field attribute.
     *
     * @param field the field info to check
     * @return true if the field is a sparse tokens field, false otherwise
     */
    public static boolean isSparseField(FieldInfo field) {
        if (field == null) {
            return false;
        }
        return field.attributes().containsKey(SPARSE_FIELD);
    }
}
