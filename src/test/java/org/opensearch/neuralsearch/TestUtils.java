/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opensearch.test.OpenSearchTestCase.randomFloat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.query.QuerySearchResult;

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

    /**
     * Assert results of hyrdir query after score normalization and combination
     * @param querySearchResults collection of query search results after they processed by normalization processor
     */
    public static void assertQueryResultScores(List<QuerySearchResult> querySearchResults) {
        assertNotNull(querySearchResults);
        float maxScore = querySearchResults.stream()
            .map(searchResult -> searchResult.topDocs().maxScore)
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, maxScore, 0.0f);
        float totalMaxScore = querySearchResults.stream()
            .map(searchResult -> searchResult.getMaxScore())
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, totalMaxScore, 0.0f);
        float maxScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .max(Float::compare)
                    .orElse(Float.MAX_VALUE)
            )
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, maxScoreScoreFromScoreDocs, 0.0f);
        float minScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .min(Float::compare)
                    .orElse(Float.MAX_VALUE)
            )
            .min(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(0.001f, minScoreScoreFromScoreDocs, 0.0f);
    }
}
