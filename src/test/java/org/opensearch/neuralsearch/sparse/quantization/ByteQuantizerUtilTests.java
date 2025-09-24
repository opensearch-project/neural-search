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
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_INGEST;

public class ByteQuantizerUtilTests extends AbstractSparseTestBase {

    public void testGetUnsignedByte() {
        // Test positive byte values
        Assert.assertEquals(0, ByteQuantizerUtil.getUnsignedByte((byte) 0));
        Assert.assertEquals(127, ByteQuantizerUtil.getUnsignedByte((byte) 127));

        // Test negative byte values (which represent unsigned values 128-255)
        Assert.assertEquals(128, ByteQuantizerUtil.getUnsignedByte((byte) -128));
        Assert.assertEquals(255, ByteQuantizerUtil.getUnsignedByte((byte) -1));
    }

    public void testCompareUnsignedByte() {
        // Test equal values
        Assert.assertEquals(0, ByteQuantizerUtil.compareUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizerUtil.compareUnsignedByte((byte) 127, (byte) 127));
        Assert.assertEquals(0, ByteQuantizerUtil.compareUnsignedByte((byte) -1, (byte) -1));

        // Test first value less than second
        Assert.assertTrue(ByteQuantizerUtil.compareUnsignedByte((byte) 0, (byte) 1) < 0);
        Assert.assertTrue(ByteQuantizerUtil.compareUnsignedByte((byte) 127, (byte) -128) < 0);

        // Test first value greater than second
        Assert.assertTrue(ByteQuantizerUtil.compareUnsignedByte((byte) 1, (byte) 0) > 0);
        Assert.assertTrue(ByteQuantizerUtil.compareUnsignedByte((byte) -1, (byte) 0) > 0);
    }

    public void testMultiplyUnsignedByte() {
        // Test multiplication with zero
        Assert.assertEquals(0, ByteQuantizerUtil.multiplyUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizerUtil.multiplyUnsignedByte((byte) 0, (byte) 255));

        // Test multiplication with positive values
        Assert.assertEquals(1, ByteQuantizerUtil.multiplyUnsignedByte((byte) 1, (byte) 1));
        Assert.assertEquals(25, ByteQuantizerUtil.multiplyUnsignedByte((byte) 5, (byte) 5));

        // Test multiplication with values that would be negative as signed bytes
        Assert.assertEquals(255, ByteQuantizerUtil.multiplyUnsignedByte((byte) 1, (byte) -1));
        Assert.assertEquals(128 * 128, ByteQuantizerUtil.multiplyUnsignedByte((byte) -128, (byte) -128));
        Assert.assertEquals(255 * 255, ByteQuantizerUtil.multiplyUnsignedByte((byte) -1, (byte) -1));
    }

    public void testGetSimScorer() {
        // Test with boost = 1.0f
        Similarity.SimScorer scorer = ByteQuantizerUtil.getSimScorer(1.0f, 3.0f, 3.0f);
        float expectedRatio = 3.0f * 3.0f / 255.0f / 255.0f;

        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(expectedRatio, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2 * expectedRatio, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);

        // Test with boost = 2.0f
        scorer = ByteQuantizerUtil.getSimScorer(2.0f, 3.0f, 3.0f);
        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2 * expectedRatio, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(4 * expectedRatio, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);
    }

    public void testGetByteQuantizerIngest_withNullSegmentInfo() {
        ByteQuantizer quantizer = ByteQuantizerUtil.getByteQuantizerIngest(null);

        assertNotNull(quantizer);
        assertEquals(255, ByteQuantizerUtil.getUnsignedByte(quantizer.quantize(DEFAULT_QUANTIZATION_CEILING_INGEST)));
    }

    public void testGetByteQuantizerIngest_withNullAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn(null);

        ByteQuantizer quantizer = ByteQuantizerUtil.getByteQuantizerIngest(fieldInfo);

        assertNotNull(quantizer);
        assertEquals(255, ByteQuantizerUtil.getUnsignedByte(quantizer.quantize(DEFAULT_QUANTIZATION_CEILING_INGEST)));
    }

    public void testGetByteQuantizerIngest_withValidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("5.0");

        ByteQuantizer quantizer = ByteQuantizerUtil.getByteQuantizerIngest(fieldInfo);

        assertNotNull(quantizer);
        assertEquals(255, ByteQuantizerUtil.getUnsignedByte(quantizer.quantize(5.0f)));
        assertEquals(128, ByteQuantizerUtil.getUnsignedByte(quantizer.quantize(2.5f)));
    }

    public void testGetByteQuantizerIngest_withInvalidAttribute() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("invalid");

        NumberFormatException exception = assertThrows(
            NumberFormatException.class,
            () -> ByteQuantizerUtil.getByteQuantizerIngest(fieldInfo)
        );
        assertTrue(exception.getMessage().contains("invalid"));
    }
}
