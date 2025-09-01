/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseDocValuesReaderTests extends OpenSearchTestCase {

    private SparseDocValuesReader sparseDocValuesReader;
    private FieldInfo fieldInfo;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getName()).thenReturn("test_field");
    }

    public void testGetBinary_StandardMergeState() throws IOException {
        // Setup with standard merge state
        boolean isWithLiveDocs = false;
        boolean isNullLiveDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeStateWithMockedBinaryDocValues(isWithLiveDocs, isNullLiveDocs);
        sparseDocValuesReader = new SparseDocValuesReader(mergeState);

        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparseBinaryDocValues);
        assertEquals(10L, ((SparseBinaryDocValues) result).getTotalLiveDocs()); // return cost here
    }

    public void testGetBinary_MergeStateWithoutLiveDocs() throws IOException {
        // Setup with merge state without LiveDocs
        MergeState mergeState = TestsPrepareUtils.prepareMergeStateWithMockedBinaryDocValues(false, true);
        sparseDocValuesReader = new SparseDocValuesReader(mergeState);

        expectThrows(NullPointerException.class, () -> { BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo); });
    }

    public void testGetBinary_WithLiveDocs() throws IOException {
        // Setup with merge state that has live docs
        boolean isWithLiveDocs = true;
        boolean isNullLiveDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeStateWithMockedBinaryDocValues(isWithLiveDocs, isNullLiveDocs);
        sparseDocValuesReader = new SparseDocValuesReader(mergeState);

        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparseBinaryDocValues);
        assertEquals(5L, ((SparseBinaryDocValues) result).getTotalLiveDocs()); // Only 5 docs (0,2,4,6,8) are live
    }

    public void testGetBinary_WithPassThroughValues() throws IOException {
        // Setup with merge state that has SparseBinaryDocValuesPassThrough
        boolean isWithLiveDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeStateWithPassThroughValues(isWithLiveDocs);
        sparseDocValuesReader = new SparseDocValuesReader(mergeState);

        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparseBinaryDocValues);
        assertEquals(10L, ((SparseBinaryDocValues) result).getTotalLiveDocs()); // return cost here
    }

    public void testGetMergeState() throws IOException {
        boolean isWithLiveDocs = true;
        boolean isNullLiveDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeStateWithMockedBinaryDocValues(isWithLiveDocs, isNullLiveDocs);
        sparseDocValuesReader = new SparseDocValuesReader(mergeState);
        assertEquals(mergeState, sparseDocValuesReader.getMergeState());
    }
}
