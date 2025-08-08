/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;

public class PredicateUtilsTests extends AbstractSparseTestBase {

    private static SegmentInfo segmentInfo;
    private static FieldInfo fieldInfo;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

    }

    public void testShouldRunSeisPredicate_withDocCountAboveThreshold_returnsTrue() {

        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "5");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) >= threshold (5)
        assertTrue("Should return true when doc count is above threshold", result);
    }

    public void testShouldRunSeisPredicate_withDocCountEqualToThreshold_returnsTrue() {
        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "10");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) >= threshold (10)
        assertTrue("Should return true when doc count equals threshold", result);
    }

    public void testShouldRunSeisPredicate_withDocCountBelowThreshold_returnsFalse() {

        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "15");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) < threshold (15)
        assertFalse("Should return false when doc count is below threshold", result);
    }

    public void testShouldRunSeisPredicate_withInvalidThreshold_throwsException() {

        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "invalid_number");

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertEquals("For input string: \"invalid_number\"", exception.getMessage());
    }

    public void testShouldRunSeisPredicate_withMissingAttribute_throwsException() {

        // No APPROXIMATE_THRESHOLD_FIELD

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertEquals("Cannot parse null string", exception.getMessage());
    }

    public void testShouldRunSeisPredicate_withNullAttribute_throwsException() {

        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, null);

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertEquals("Cannot parse null string", exception.getMessage());
    }

    public void testShouldRunSeisPredicate_isNotNull() {
        assertNotNull("shouldRunSeisPredicate should not be null", PredicateUtils.shouldRunSeisPredicate);
    }

    public void testShouldRunSeisPredicate_isBiPredicate() {
        assertTrue(
            "shouldRunSeisPredicate should be instance of BiPredicate",
            PredicateUtils.shouldRunSeisPredicate instanceof java.util.function.BiPredicate
        );
    }
}
