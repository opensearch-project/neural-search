/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.common;

import java.util.Collections;
import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

public class VectorUtilTests extends OpenSearchTestCase {

    public void testVectorAsListToArray() {
        List<Float> vectorAsList_withThreeElements = List.of(1.3f, 2.5f, 3.5f);
        float[] vectorAsArray_withThreeElements = VectorUtil.vectorAsListToArray(vectorAsList_withThreeElements);

        assertEquals(vectorAsList_withThreeElements.size(), vectorAsArray_withThreeElements.length);
        for (int i = 0; i < vectorAsList_withThreeElements.size(); i++) {
            assertEquals(vectorAsList_withThreeElements.get(i), vectorAsArray_withThreeElements[i], 0.0f);
        }

        List<Float> vectorAsList_withNoElements = Collections.emptyList();
        float[] vectorAsArray_withNoElements = VectorUtil.vectorAsListToArray(vectorAsList_withNoElements);
        assertEquals(0, vectorAsArray_withNoElements.length);
    }

}
