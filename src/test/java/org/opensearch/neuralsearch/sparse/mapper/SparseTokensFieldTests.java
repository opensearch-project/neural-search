/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class SparseTokensFieldTests extends AbstractSparseTestBase {

    private final static String fieldName = "testField";
    private final static byte[] testValue = new byte[] { 1, 2, 3 };
    private final static IndexableFieldType mockType = TestsPrepareUtils.prepareIndexableFieldType();

    public void testSparseTokensFieldConstructor() {
        SparseTokensField field = new SparseTokensField(fieldName, testValue, mockType);

        assertNotNull("Field should be created successfully", field);
        assertEquals("Field name should match", fieldName, field.name());
        assertArrayEquals("Binary value should match", testValue, field.binaryValue().bytes);
        assertEquals("Field type should match", mockType, field.fieldType());
    }

    public void testIsSparseFieldReturnsFalseWhenFieldIsNull() {
        FieldInfo field = null;
        assertFalse("Should return false for null field", SparseTokensField.isSparseField(field));
    }

    public void testIsSparseFieldWhenFieldContainsSparseAttribute() throws Exception {
        FieldInfo mockField = TestsPrepareUtils.prepareKeyFieldInfo();
        mockField.putAttribute(SPARSE_FIELD, "true");

        boolean result = SparseTokensField.isSparseField(mockField);

        assertTrue("Should return true for field with sparse attribute", result);
    }

    public void testIsSparseFieldWithNullField() {
        assertFalse("Should return false for null field", SparseTokensField.isSparseField(null));
    }

    public void testIsSparseFieldWithFalseResult() {
        FieldInfo mockField = TestsPrepareUtils.prepareKeyFieldInfo();
        mockField.putAttribute(SPARSE_FIELD, "false");

        boolean result = SparseTokensField.isSparseField(mockField);

        assertFalse("Should return false for field with false attribute", result);
    }
}
