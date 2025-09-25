/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.quantization;

import org.junit.Assert;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class ByteQuantizerTests extends AbstractSparseTestBase {

    public void testConstructor_withNegativeValue_thenThrowsException() {
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> new ByteQuantizer(-1.0f));
        assertEquals("Ceiling value must be positive for byte quantizer", exception.getMessage());
    }

    public void testQuantize() {
        ByteQuantizer byteQuantizer = new ByteQuantizer(3.0f);

        // Test minimum value (0.0f)
        Assert.assertEquals(0, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(0.0f)));

        // Test maximum value (3.0f)
        Assert.assertEquals(255, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(3.0f)));

        // Test middle value (1.5f)
        Assert.assertEquals(128, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(1.5f)));

        // Test value below minimum (should be clamped to 0.0f)
        Assert.assertEquals(0, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(-1.0f)));

        // Test value above maximum (should be clamped to 3.0f)
        Assert.assertEquals(255, ByteQuantizationUtil.getUnsignedByte(byteQuantizer.quantize(4.0f)));
    }
}
