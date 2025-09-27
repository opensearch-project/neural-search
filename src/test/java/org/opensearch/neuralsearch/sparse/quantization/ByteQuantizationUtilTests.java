/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.quantization;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.Assert;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_INGEST;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_SEARCH;

public class ByteQuantizationUtilTests extends AbstractSparseTestBase {

    public void testGetUnsignedByte() {
        // Test positive byte values
        Assert.assertEquals(0, ByteQuantizationUtil.getUnsignedByte((byte) 0));
        Assert.assertEquals(127, ByteQuantizationUtil.getUnsignedByte((byte) 127));

        // Test negative byte values (which represent unsigned values 128-255)
        Assert.assertEquals(128, ByteQuantizationUtil.getUnsignedByte((byte) -128));
        Assert.assertEquals(255, ByteQuantizationUtil.getUnsignedByte((byte) -1));
    }

    public void testCompareUnsignedByte() {
        // Test equal values
        Assert.assertEquals(0, ByteQuantizationUtil.compareUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizationUtil.compareUnsignedByte((byte) 127, (byte) 127));
        Assert.assertEquals(0, ByteQuantizationUtil.compareUnsignedByte((byte) -1, (byte) -1));

        // Test first value less than second
        Assert.assertTrue(ByteQuantizationUtil.compareUnsignedByte((byte) 0, (byte) 1) < 0);
        Assert.assertTrue(ByteQuantizationUtil.compareUnsignedByte((byte) 127, (byte) -128) < 0);

        // Test first value greater than second
        Assert.assertTrue(ByteQuantizationUtil.compareUnsignedByte((byte) 1, (byte) 0) > 0);
        Assert.assertTrue(ByteQuantizationUtil.compareUnsignedByte((byte) -1, (byte) 0) > 0);
    }

    public void testMultiplyUnsignedByte() {
        // Test multiplication with zero
        Assert.assertEquals(0, ByteQuantizationUtil.multiplyUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizationUtil.multiplyUnsignedByte((byte) 0, (byte) 255));

        // Test multiplication with positive values
        Assert.assertEquals(1, ByteQuantizationUtil.multiplyUnsignedByte((byte) 1, (byte) 1));
        Assert.assertEquals(25, ByteQuantizationUtil.multiplyUnsignedByte((byte) 5, (byte) 5));

        // Test multiplication with values that would be negative as signed bytes
        Assert.assertEquals(255, ByteQuantizationUtil.multiplyUnsignedByte((byte) 1, (byte) -1));
        Assert.assertEquals(128 * 128, ByteQuantizationUtil.multiplyUnsignedByte((byte) -128, (byte) -128));
        Assert.assertEquals(255 * 255, ByteQuantizationUtil.multiplyUnsignedByte((byte) -1, (byte) -1));
    }

    public void testGetSimScorer() {
        // Test with boost = 1.0f
        float boost = 1.0f;
        Similarity.SimScorer scorer = ByteQuantizationUtil.getSimScorer(boost);

        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(boost, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2.0f * boost, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);

        // Test with boost = 2.0f
        boost = 2.0f;
        scorer = ByteQuantizationUtil.getSimScorer(boost);
        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(boost, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2.0f * boost, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueIngest_withNullFieldInfo() {
        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueIngest(null);

        assertEquals(DEFAULT_QUANTIZATION_CEILING_INGEST, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueIngest_withNullAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn(null);

        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueIngest(null);

        assertEquals(DEFAULT_QUANTIZATION_CEILING_INGEST, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueIngest_withValidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("5.0");

        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueIngest(fieldInfo);

        assertEquals(5.0f, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueIngest_withInvalidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("invalid");

        NumberFormatException exception = assertThrows(
            NumberFormatException.class,
            () -> ByteQuantizationUtil.getByteQuantizerIngest(fieldInfo)
        );
        assertTrue(exception.getMessage().contains("invalid"));
    }

    public void testGetCeilingValueSearch_withNullFieldInfo() {
        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueSearch(null);

        assertEquals(DEFAULT_QUANTIZATION_CEILING_SEARCH, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueSearch_withNullAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD)).thenReturn(null);

        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueSearch(null);

        assertEquals(DEFAULT_QUANTIZATION_CEILING_SEARCH, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueSearch_withValidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD)).thenReturn("5.0");

        float ceilingValueIngest = ByteQuantizationUtil.getCeilingValueSearch(fieldInfo);

        assertEquals(5.0f, ceilingValueIngest, DELTA_FOR_ASSERTION);
    }

    public void testGetCeilingValueSearch_withInvalidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD)).thenReturn("invalid");

        NumberFormatException exception = assertThrows(
            NumberFormatException.class,
            () -> ByteQuantizationUtil.getCeilingValueSearch(fieldInfo)
        );
        assertTrue(exception.getMessage().contains("invalid"));
    }

    public void testGetByteQuantizerIngest_withValidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("5.0");

        ByteQuantizer byteQuantizer = ByteQuantizationUtil.getByteQuantizerIngest(fieldInfo);

        assertNotNull(byteQuantizer);
        assertEquals(255, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(5.0f)), DELTA_FOR_ASSERTION);
        assertEquals(128, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(2.5f)), DELTA_FOR_ASSERTION);
    }
}
