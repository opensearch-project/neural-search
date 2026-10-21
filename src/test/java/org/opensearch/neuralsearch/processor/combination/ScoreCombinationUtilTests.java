/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;

public class ScoreCombinationUtilTests extends OpenSearchTestCase {

    private static final float DELTA = 0.01f;
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);

    public void testGetWeights_whenNullOrEmptyParams_thenReturnsEmptyList() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        assertTrue(util.getWeights(null).isEmpty());
        assertTrue(util.getWeights(Map.of()).isEmpty());
    }

    public void testGetWeights_whenDoubleWeights_thenReturnsFloats() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        List<Float> weights = util.getWeights(Map.of(PARAM_NAME_WEIGHTS, List.of(0.4, 0.6)));
        assertEquals(2, weights.size());
        assertEquals(0.4f, weights.get(0), DELTA);
        assertEquals(0.6f, weights.get(1), DELTA);
    }

    public void testGetWeights_whenIntegerWeights_thenCoercedWithoutClassCastException() {
        // A weight provided as a JSON integer (e.g. "weights": [1]) deserializes to Integer, not Double.
        // Before the fix this threw a ClassCastException (Integer cannot be cast to Double) surfacing as HTTP 500.
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        List<Float> weights = util.getWeights(Map.of(PARAM_NAME_WEIGHTS, List.of(1)));
        assertEquals(1, weights.size());
        assertEquals(1.0f, weights.get(0), DELTA);
    }

    public void testGetWeights_whenMixedNumericWeights_thenCoerced() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        List<Float> weights = util.getWeights(Map.of(PARAM_NAME_WEIGHTS, List.of(0, 1)));
        assertEquals(2, weights.size());
        assertEquals(0.0f, weights.get(0), DELTA);
        assertEquals(1.0f, weights.get(1), DELTA);
    }

    public void testValidateParams_whenDoubleWeights_thenNoException() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        // should not throw
        util.validateParams(Map.of(PARAM_NAME_WEIGHTS, List.of(0.3, 0.7)), SUPPORTED_PARAMS);
    }

    public void testValidateParams_whenIntegerWeights_thenNoException() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        // integers are numbers, so validation must accept them (getWeights coerces them to float)
        util.validateParams(Map.of(PARAM_NAME_WEIGHTS, List.of(0, 1)), SUPPORTED_PARAMS);
    }

    public void testValidateParams_whenStringWeight_thenThrowsIllegalArgumentException() {
        // A weight provided as a JSON string (e.g. "weights": ["0.5"]) must be rejected as a bad request (HTTP 400)
        // via IllegalArgumentException, not leak through to a ClassCastException (HTTP 500).
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> util.validateParams(Map.of(PARAM_NAME_WEIGHTS, List.of("0.5")), SUPPORTED_PARAMS)
        );
        assertEquals("parameter [weights] must be a collection of numbers", exception.getMessage());
    }

    public void testValidateParams_whenMixedNumberAndStringWeights_thenThrowsIllegalArgumentException() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> util.validateParams(Map.of(PARAM_NAME_WEIGHTS, List.of(0.5, "0.5")), SUPPORTED_PARAMS)
        );
        assertEquals("parameter [weights] must be a collection of numbers", exception.getMessage());
    }

    public void testValidateParams_whenWeightsNotAList_thenThrowsIllegalArgumentException() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> util.validateParams(Map.of(PARAM_NAME_WEIGHTS, "0.5"), SUPPORTED_PARAMS)
        );
        assertEquals("parameter [weights] must be a collection of numbers", exception.getMessage());
    }

    public void testValidateParams_whenUnsupportedParam_thenThrowsIllegalArgumentException() {
        ScoreCombinationUtil util = new ScoreCombinationUtil();
        expectThrows(
            IllegalArgumentException.class,
            () -> util.validateParams(Map.of("unsupported_param", List.of(0.5)), SUPPORTED_PARAMS)
        );
    }
}
