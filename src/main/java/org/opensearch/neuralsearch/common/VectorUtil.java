/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.common;

import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Utility class for working with vectors
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VectorUtil {

    /**
     * Converts a vector represented as a list to an array
     *
     * @param vectorAsList {@link List} of {@link Float}'s representing the vector
     * @return array of floats produced from input list
     */
    public static float[] vectorAsListToArray(List<Number> vectorAsList) {
        float[] vector = new float[vectorAsList.size()];
        for (int i = 0; i < vectorAsList.size(); i++) {
            vector[i] = vectorAsList.get(i).floatValue();
        }
        return vector;
    }
}
