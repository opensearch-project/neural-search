/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;

public class SparseTokensFieldTests extends AbstractSparseTestBase {

    private final String fieldName = "testField";
    private final byte[] testValue = new byte[] { 1, 2, 3 };
    private final IndexableFieldType mockType = testsPrepareUtils.prepareMockIndexableFieldType();

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
        testsPrepareUtils prepareHelper = new testsPrepareUtils();
        FieldInfo mockField = prepareHelper.prepareKeyFieldInfo();
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SPARSE_FIELD, "true");

        Field attributesField = FieldInfo.class.getDeclaredField("attributes");
        attributesField.setAccessible(true);
        attributesField.set(mockField, attributes);

        boolean result = SparseTokensField.isSparseField(mockField);

        assertTrue("Should return true for field with sparse attribute", result);
    }

    public void testIsSparseFieldWithNullField() {
        assertFalse("Should return false for null field", SparseTokensField.isSparseField(null));
    }

}
