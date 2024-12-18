/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.prune;

import org.opensearch.test.OpenSearchTestCase;

public class PruneTypeTests extends OpenSearchTestCase {
    public void testGetValue() {
        assertEquals("none", PruneType.NONE.getValue());
        assertEquals("top_k", PruneType.TOP_K.getValue());
        assertEquals("alpha_mass", PruneType.ALPHA_MASS.getValue());
        assertEquals("max_ratio", PruneType.MAX_RATIO.getValue());
        assertEquals("abs_value", PruneType.ABS_VALUE.getValue());
    }

    public void testFromString() {
        assertEquals(PruneType.NONE, PruneType.fromString("none"));
        assertEquals(PruneType.NONE, PruneType.fromString(null));
        assertEquals(PruneType.NONE, PruneType.fromString(""));
        assertEquals(PruneType.TOP_K, PruneType.fromString("top_k"));
        assertEquals(PruneType.ALPHA_MASS, PruneType.fromString("alpha_mass"));
        assertEquals(PruneType.MAX_RATIO, PruneType.fromString("max_ratio"));
        assertEquals(PruneType.ABS_VALUE, PruneType.fromString("abs_value"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PruneType.fromString("test_value"));
        assertEquals("Unknown prune type: test_value", exception.getMessage());
    }
}
