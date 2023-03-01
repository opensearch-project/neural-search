/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch;

import static org.opensearch.test.OpenSearchTestCase.randomFloat;

import java.util.Map;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentBuilder;

public class TestUtils {

    /**
     * Convert an xContentBuilder to a map
     * @param xContentBuilder to produce map from
     * @return Map from xContentBuilder
     */
    public static Map<String, Object> xContentBuilderToMap(XContentBuilder xContentBuilder) {
        return XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2();
    }

    /**
     * Utility method to convert an object to a float
     *
     * @param obj object to be converted to float
     * @return object as float
     */
    public static Float objectToFloat(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        }

        throw new IllegalArgumentException("Object provided must be of type Number");
    }

    /**
     * Create a random vector of provided dimension
     *
     * @param dimension of vector to be created
     * @return dimension-dimensional floating point array with random content
     */
    public static float[] createRandomVector(int dimension) {
        float[] vector = new float[dimension];
        for (int j = 0; j < dimension; j++) {
            vector[j] = randomFloat();
        }
        return vector;
    }
}
