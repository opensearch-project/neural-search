/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_FLOATS_ASSERTION;

public class ScoreNormalizationUtilTests extends OpenSearchTestCase {

    private ScoreNormalizationUtil scoreNormalizationUtil;
    private Set<String> supportedTopLevelParams;
    private Map<String, Set<String>> supportedNestedParams;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scoreNormalizationUtil = new ScoreNormalizationUtil();
        supportedTopLevelParams = new HashSet<>(Arrays.asList("method", "parameters"));
        supportedNestedParams = new HashMap<>();
        supportedNestedParams.put("parameters", new HashSet<>(Arrays.asList("factor", "offset")));
    }

    public void testValidateParamsWithUnsupportedParameter() {
        Map<String, Object> actualParams = new HashMap<>();
        actualParams.put("unsupported_param", "value");
        Set<String> supportedParams = new HashSet<>(List.of("weights"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParams(actualParams, supportedParams)
        );
        assertTrue(exception.getMessage().contains("parameter for combination technique is not supported"));
    }

    public void testValidateParamsWithInvalidWeightsType() {
        Map<String, Object> actualParams = new HashMap<>();
        actualParams.put("weights", "invalid_type");
        Set<String> supportedParams = new HashSet<>(List.of("weights"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParams(actualParams, supportedParams)
        );
        assertTrue(exception.getMessage().contains("parameter [weights] must be a collection of numbers"));
    }

    public void testSetNormalizedScore() {
        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
        SearchShard searchShard = new SearchShard("index1", 0, "shard1");
        DocIdAtSearchShard docId = new DocIdAtSearchShard(1, searchShard);
        int subQueryIndex = 1;
        int numberOfSubQueries = 3;
        float normalizedScore = 0.75f;

        ScoreNormalizationUtil.setNormalizedScore(normalizedScores, docId, subQueryIndex, numberOfSubQueries, normalizedScore);

        assertTrue(normalizedScores.containsKey(docId));
        List<Float> scores = normalizedScores.get(docId);
        assertEquals(numberOfSubQueries, scores.size());
        assertEquals(normalizedScore, scores.get(subQueryIndex), DELTA_FOR_FLOATS_ASSERTION);
        assertEquals(0.0f, scores.get(0), DELTA_FOR_FLOATS_ASSERTION);
        assertEquals(0.0f, scores.get(2), DELTA_FOR_FLOATS_ASSERTION);
    }

    public void testValidateParametersWithNullParameters() {
        scoreNormalizationUtil.validateParameters(null, supportedTopLevelParams, supportedNestedParams);
    }

    public void testValidateParametersWithEmptyParameters() {
        scoreNormalizationUtil.validateParameters(new HashMap<>(), supportedTopLevelParams, supportedNestedParams);
    }

    public void testValidateParametersWithValidTopLevelParameters() {
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("factor", 1.0);
        params.put("parameters", nestedParams);

        scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams);
    }

    public void testValidateParametersWithValidNestedParameters() {
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("factor", 1.0);
        nestedParams.put("offset", 0.0);

        Map<String, Object> params = new HashMap<>();
        params.put("parameters", nestedParams);

        scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams);
    }

    public void testValidateParametersWithInvalidTopLevelParameter() {
        Map<String, Object> params = new HashMap<>();
        params.put("invalid_param", "value");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams)
        );
        assertEquals("unrecognized parameters in normalization technique", exception.getMessage());
    }

    public void testValidateParametersWithInvalidNestedParameter() {
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("invalid_nested", "value");

        Map<String, Object> params = new HashMap<>();
        params.put("parameters", nestedParams);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams)
        );
        assertEquals("unrecognized parameters in normalization technique", exception.getMessage());
    }

    public void testValidateParametersWithListOfMaps() {
        Map<String, Object> validNestedParams1 = new HashMap<>();
        validNestedParams1.put("factor", 1.0);
        Map<String, Object> validNestedParams2 = new HashMap<>();
        validNestedParams2.put("offset", 0.0);

        Map<String, Object> params = new HashMap<>();
        params.put("parameters", Arrays.asList(validNestedParams1, validNestedParams2));

        scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams);
    }

    public void testValidateParametersWithInvalidListContent() {
        Map<String, Object> params = new HashMap<>();
        params.put("parameters", Arrays.asList("invalid", "content"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams)
        );
        assertEquals("unrecognized parameters in normalization technique", exception.getMessage());
    }

    public void testValidateParametersWithMixedValidAndInvalidParameters() {
        Map<String, Object> nestedParams = new HashMap<>();
        nestedParams.put("factor", 1.0);
        nestedParams.put("invalid_nested", "value");

        Map<String, Object> params = new HashMap<>();
        params.put("method", "min_max");
        params.put("parameters", nestedParams);
        params.put("invalid_top", "value");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> scoreNormalizationUtil.validateParameters(params, supportedTopLevelParams, supportedNestedParams)
        );
        assertEquals("unrecognized parameters in normalization technique", exception.getMessage());
    }
}
