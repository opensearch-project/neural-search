/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import org.opensearch.test.OpenSearchTestCase;

public class BoundModeTests extends OpenSearchTestCase {

    public void testDefaultValue_whenAccessed_thenSuccessful() {
        assertEquals(BoundMode.APPLY, BoundMode.DEFAULT);
    }

    public void testGetValidValues_whenCalled_thenSuccessful() {
        assertEquals("apply, clip, ignore", BoundMode.getValidValues());
    }

    public void testFromString_whenValidInput_thenSuccessful() {
        // Test uppercase inputs
        assertEquals(BoundMode.APPLY, BoundMode.fromString("APPLY"));
        assertEquals(BoundMode.CLIP, BoundMode.fromString("CLIP"));
        assertEquals(BoundMode.IGNORE, BoundMode.fromString("IGNORE"));

        // Test lowercase inputs
        assertEquals(BoundMode.APPLY, BoundMode.fromString("apply"));
        assertEquals(BoundMode.CLIP, BoundMode.fromString("clip"));
        assertEquals(BoundMode.IGNORE, BoundMode.fromString("ignore"));
    }

    public void testFromString_whenInvalidInput_thenFail() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> BoundMode.fromString("invalid"));
        assertEquals("invalid mode: invalid, valid values are: apply, clip, ignore", exception.getMessage());
    }

    public void testFromString_whenNullOrEmptyInput_thenReturnDefault() {
        assertEquals(BoundMode.DEFAULT, BoundMode.fromString("  "));
        assertEquals(BoundMode.DEFAULT, BoundMode.fromString(null));
    }

    public void testToString() {
        assertEquals("apply", BoundMode.APPLY.toString());
        assertEquals("clip", BoundMode.CLIP.toString());
        assertEquals("ignore", BoundMode.IGNORE.toString());
    }
}
