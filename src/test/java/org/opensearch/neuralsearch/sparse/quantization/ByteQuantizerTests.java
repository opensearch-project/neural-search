/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.quantization;

import org.apache.lucene.search.similarities.Similarity;
import org.junit.Assert;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class ByteQuantizerTests extends AbstractSparseTestBase {

    public void testQuantizeFloatToByte() {
        // Test minimum value (0.0f)
        Assert.assertEquals(0, ByteQuantizer.getUnsignedByte(ByteQuantizer.quantizeFloatToByte(0.0f)));

        // Test maximum value (3.0f)
        Assert.assertEquals(255, ByteQuantizer.getUnsignedByte(ByteQuantizer.quantizeFloatToByte(3.0f)));

        // Test middle value (1.5f)
        Assert.assertEquals(128, ByteQuantizer.getUnsignedByte(ByteQuantizer.quantizeFloatToByte(1.5f)));

        // Test value below minimum (should be clamped to 0.0f)
        Assert.assertEquals(0, ByteQuantizer.getUnsignedByte(ByteQuantizer.quantizeFloatToByte(-1.0f)));

        // Test value above maximum (should be clamped to 3.0f)
        Assert.assertEquals(255, ByteQuantizer.getUnsignedByte(ByteQuantizer.quantizeFloatToByte(4.0f)));
    }

    public void testGetUnsignedByte() {
        // Test positive byte values
        Assert.assertEquals(0, ByteQuantizer.getUnsignedByte((byte) 0));
        Assert.assertEquals(127, ByteQuantizer.getUnsignedByte((byte) 127));

        // Test negative byte values (which represent unsigned values 128-255)
        Assert.assertEquals(128, ByteQuantizer.getUnsignedByte((byte) -128));
        Assert.assertEquals(255, ByteQuantizer.getUnsignedByte((byte) -1));
    }

    public void testCompareUnsignedByte() {
        // Test equal values
        Assert.assertEquals(0, ByteQuantizer.compareUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizer.compareUnsignedByte((byte) 127, (byte) 127));
        Assert.assertEquals(0, ByteQuantizer.compareUnsignedByte((byte) -1, (byte) -1));

        // Test first value less than second
        Assert.assertTrue(ByteQuantizer.compareUnsignedByte((byte) 0, (byte) 1) < 0);
        Assert.assertTrue(ByteQuantizer.compareUnsignedByte((byte) 127, (byte) -128) < 0);

        // Test first value greater than second
        Assert.assertTrue(ByteQuantizer.compareUnsignedByte((byte) 1, (byte) 0) > 0);
        Assert.assertTrue(ByteQuantizer.compareUnsignedByte((byte) -1, (byte) 0) > 0);
    }

    public void testMultiplyUnsignedByte() {
        // Test multiplication with zero
        Assert.assertEquals(0, ByteQuantizer.multiplyUnsignedByte((byte) 0, (byte) 0));
        Assert.assertEquals(0, ByteQuantizer.multiplyUnsignedByte((byte) 0, (byte) 255));

        // Test multiplication with positive values
        Assert.assertEquals(1, ByteQuantizer.multiplyUnsignedByte((byte) 1, (byte) 1));
        Assert.assertEquals(25, ByteQuantizer.multiplyUnsignedByte((byte) 5, (byte) 5));

        // Test multiplication with values that would be negative as signed bytes
        Assert.assertEquals(255, ByteQuantizer.multiplyUnsignedByte((byte) 1, (byte) -1));
        Assert.assertEquals(128 * 128, ByteQuantizer.multiplyUnsignedByte((byte) -128, (byte) -128));
        Assert.assertEquals(255 * 255, ByteQuantizer.multiplyUnsignedByte((byte) -1, (byte) -1));
    }

    public void testGetSimScorer() {
        // Test with boost = 1.0f
        Similarity.SimScorer scorer = ByteQuantizer.getSimScorer(1.0f);
        float expectedRatio = ByteQuantizer.SCORE_RESCALE_RATIO;

        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(expectedRatio, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2 * expectedRatio, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);

        // Test with boost = 2.0f
        scorer = ByteQuantizer.getSimScorer(2.0f);
        Assert.assertEquals(0.0f, scorer.score(0.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(2 * expectedRatio, scorer.score(1.0f, 0), DELTA_FOR_ASSERTION);
        Assert.assertEquals(4 * expectedRatio, scorer.score(2.0f, 0), DELTA_FOR_ASSERTION);
    }

    public void testScoreRescaleRatio() {
        // Verify the constant is calculated correctly
        float maxFloatValue = 3.0f;
        float maxUnsignedByteValue = 255.0f;
        float expectedRatio = maxFloatValue * maxFloatValue / maxUnsignedByteValue / maxUnsignedByteValue;

        Assert.assertEquals(expectedRatio, ByteQuantizer.SCORE_RESCALE_RATIO, DELTA_FOR_ASSERTION);
    }
}
