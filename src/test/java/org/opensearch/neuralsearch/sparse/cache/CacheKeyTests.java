/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

public class CacheKeyTests extends AbstractSparseTestBase {

    private static final String testFieldName = "test_field";
    private static final FieldInfo testFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
    private static final SegmentInfo testSegmentInfo = TestsPrepareUtils.prepareSegmentInfo();

    /**
     * Tests that the CacheKey constructor with FieldInfo parameter creates a valid instance.
     * This verifies the basic construction functionality with FieldInfo.
     */
    public void test_constructor_withFieldInfo_thenCreatesCorrectly() {
        CacheKey cacheKey = new CacheKey(testSegmentInfo, testFieldInfo);

        assertNotNull("CacheKey should be created", cacheKey);
    }

    /**
     * Tests that the CacheKey constructor with field name parameter creates a valid instance.
     * This verifies the basic construction functionality with field name.
     */
    public void test_constructor_withFieldName_thenCreatesCorrectly() {
        CacheKey cacheKey = new CacheKey(testSegmentInfo, testFieldName);

        assertNotNull("CacheKey should be created", cacheKey);
    }

    /**
     * Tests that the CacheKey constructor throws NullPointerException when segmentInfo is null and fieldInfo is provided.
     * This verifies the @NonNull annotation on the segmentInfo parameter.
     */
    public void test_constructor_withNullSegmentInfoAndFieldInfo_thenThrowNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, testFieldInfo); });

        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that the CacheKey constructor throws NullPointerException when segmentInfo is null and fieldName is provided.
     * This verifies the @NonNull annotation on the segmentInfo parameter.
     */
    public void test_constructor_withNullSegmentInfoAndFieldName_thenThrowNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, testFieldName); });

        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that the CacheKey constructor throws NullPointerException when fieldName is null.
     * This verifies the @NonNull annotation on the fieldName parameter.
     */
    public void test_constructor_withNullFieldName_thenThrowNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(testSegmentInfo, (String) null); });

        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that the CacheKey constructor throws NullPointerException when fieldInfo is null.
     * This verifies the @NonNull annotation on the fieldInfo parameter.
     */
    public void test_constructor_withNullFieldInfo_thenThrowNullPointerException() {
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> { new CacheKey(testSegmentInfo, (FieldInfo) null); }
        );

        assertEquals("fieldInfo is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that the CacheKey constructor throws NullPointerException when both segmentInfo and fieldInfo are null.
     * This verifies that the first @NonNull check (on segmentInfo) is triggered before checking fieldInfo.
     */
    public void test_constructor_withBothNullFieldInfo_thenThrowNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, (FieldInfo) null); });
        // Trigger first parameter NonNull check
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that the CacheKey constructor with FieldInfo correctly extracts the field name.
     * This verifies that the field name extraction from FieldInfo works correctly.
     */
    public void test_constructorWithFieldInfo_thenExtractsFieldName() {
        CacheKey cacheKey1 = new CacheKey(testSegmentInfo, testFieldInfo);
        CacheKey cacheKey2 = new CacheKey(testSegmentInfo, testFieldInfo.getName());

        assertEquals("CacheKey created with FieldInfo should equal CacheKey created with field name", cacheKey1, cacheKey2);
    }

    /**
     * Tests that equals returns true for CacheKey instances with the same values.
     * This verifies the equality comparison for identical keys.
     */
    public void test_equals_withSameValues_thenReturnTrue() {
        CacheKey cacheKey1 = new CacheKey(testSegmentInfo, testFieldName);
        CacheKey cacheKey2 = new CacheKey(testSegmentInfo, testFieldName);

        assertEquals("CacheKeys with same values should be equal", cacheKey1, cacheKey2);
    }

    /**
     * Tests that equals returns false for CacheKey instances with different segmentInfo.
     * This verifies that the equality comparison correctly considers the segmentInfo field.
     */
    public void test_equals_withDifferentSegmentInfo_thenReturnFalse() {
        SegmentInfo segmentInfo1 = TestsPrepareUtils.prepareSegmentInfo(10);
        SegmentInfo segmentInfo2 = TestsPrepareUtils.prepareSegmentInfo(20);

        CacheKey cacheKey1 = new CacheKey(segmentInfo1, testFieldName);
        CacheKey cacheKey2 = new CacheKey(segmentInfo2, testFieldName);

        assertNotEquals("CacheKeys with different SegmentInfo should not be equal", cacheKey1, cacheKey2);
    }

    /**
     * Tests that equals returns false for CacheKey instances with different field names.
     * This verifies that the equality comparison correctly considers the fieldName field.
     */
    public void test_equals_withDifferentFieldName_thenReturnFalse() {
        CacheKey cacheKey1 = new CacheKey(testSegmentInfo, "field1");
        CacheKey cacheKey2 = new CacheKey(testSegmentInfo, "field2");

        assertNotEquals("CacheKeys with different field names should not be equal", cacheKey1, cacheKey2);
    }

    /**
     * Tests that equals returns true when comparing a CacheKey instance to itself.
     * This verifies the reflexive property of the equals method.
     */
    public void test_equals_withSameInstance_thenReturnTrue() {
        CacheKey cacheKey = new CacheKey(testSegmentInfo, testFieldName);

        assertEquals("CacheKey should equal itself", cacheKey, cacheKey);
    }

    /**
     * Tests that equals returns false when comparing a CacheKey instance to null.
     * This verifies that the equals method handles null comparisons correctly.
     */
    public void test_equals_withNull_thenReturnFalse() {
        CacheKey cacheKey = new CacheKey(testSegmentInfo, testFieldName);

        assertNotEquals("CacheKey should not equal null", cacheKey, null);
    }

    /**
     * Tests that equals returns false when comparing a CacheKey instance to an object of a different class.
     * This verifies that the equals method handles type checking correctly.
     */
    public void test_equals_withDifferentClass_thenReturnFalse() {
        CacheKey cacheKey = new CacheKey(testSegmentInfo, testFieldName);

        assertNotEquals("CacheKey should not equal different class", cacheKey, "string");
    }

    /**
     * Tests that hashCode returns the same value for CacheKey instances with the same values.
     * This verifies the consistency of the hashCode method with equals.
     */
    public void test_hashCode_withSameValues_thenReturnSame() {
        CacheKey cacheKey1 = new CacheKey(testSegmentInfo, testFieldName);
        CacheKey cacheKey2 = new CacheKey(testSegmentInfo, testFieldName);

        assertEquals("CacheKeys with same values should have same hash code", cacheKey1.hashCode(), cacheKey2.hashCode());
    }
}
