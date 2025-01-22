/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_FLOATS_ASSERTION;

public class ScoreNormalizationUtilTests extends OpenSearchTestCase {

    private ScoreNormalizationUtil scoreNormalizationUtil;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scoreNormalizationUtil = new ScoreNormalizationUtil();
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
}
