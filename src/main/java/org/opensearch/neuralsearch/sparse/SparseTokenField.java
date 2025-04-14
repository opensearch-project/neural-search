/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableFieldType;

public class SparseTokenField extends Field {
    private float tokenValue;

    public SparseTokenField(String key, float value, IndexableFieldType type) {
        super(key, type);
        this.tokenValue = value;
        this.fieldsData = value;
    }
}
