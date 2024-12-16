/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;

import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplanationUtilsTests extends OpenSearchQueryTestCase {

    private DocIdAtSearchShard docId1;
    private DocIdAtSearchShard docId2;
    private Map<DocIdAtSearchShard, List<Float>> normalizedScores;
    private final MinMaxScoreNormalizationTechnique MIN_MAX_TECHNIQUE = new MinMaxScoreNormalizationTechnique();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        SearchShard searchShard = new SearchShard("test_index", 0, "abcdefg");
        docId1 = new DocIdAtSearchShard(1, searchShard);
        docId2 = new DocIdAtSearchShard(2, searchShard);
        normalizedScores = new HashMap<>();
    }

    public void testGetDocIdAtQueryForNormalization() {
        // Setup
        normalizedScores.put(docId1, Arrays.asList(1.0f, 0.5f));
        normalizedScores.put(docId2, Arrays.asList(0.8f));
        // Act
        Map<DocIdAtSearchShard, ExplanationDetails> result = ExplanationUtils.getDocIdAtQueryForNormalization(
            normalizedScores,
            MIN_MAX_TECHNIQUE
        );
        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());

        // Assert first document
        ExplanationDetails details1 = result.get(docId1);
        assertNotNull(details1);
        List<Pair<Float, String>> explanations1 = details1.getScoreDetails();
        assertEquals(2, explanations1.size());
        assertEquals(1.0f, explanations1.get(0).getLeft(), 0.001);
        assertEquals(0.5f, explanations1.get(1).getLeft(), 0.001);
        assertEquals("min_max normalization of:", explanations1.get(0).getRight());
        assertEquals("min_max normalization of:", explanations1.get(1).getRight());

        // Assert second document
        ExplanationDetails details2 = result.get(docId2);
        assertNotNull(details2);
        List<Pair<Float, String>> explanations2 = details2.getScoreDetails();
        assertEquals(1, explanations2.size());
        assertEquals(0.8f, explanations2.get(0).getLeft(), 0.001);
        assertEquals("min_max normalization of:", explanations2.get(0).getRight());
    }

    public void testGetDocIdAtQueryForNormalizationWithEmptyScores() {
        // Setup
        // Using empty normalizedScores from setUp
        // Act
        Map<DocIdAtSearchShard, ExplanationDetails> result = ExplanationUtils.getDocIdAtQueryForNormalization(
            normalizedScores,
            MIN_MAX_TECHNIQUE
        );
        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    public void testDescribeCombinationTechniqueWithWeights() {
        // Setup
        String techniqueName = "test_technique";
        List<Float> weights = Arrays.asList(0.3f, 0.7f);
        // Act
        String result = ExplanationUtils.describeCombinationTechnique(techniqueName, weights);
        // Assert
        assertEquals("test_technique, weights [0.3, 0.7]", result);
    }

    public void testDescribeCombinationTechniqueWithoutWeights() {
        // Setup
        String techniqueName = "test_technique";
        // Act
        String result = ExplanationUtils.describeCombinationTechnique(techniqueName, null);
        // Assert
        assertEquals("test_technique", result);
    }

    public void testDescribeCombinationTechniqueWithEmptyWeights() {
        // Setup
        String techniqueName = "test_technique";
        List<Float> weights = Arrays.asList();
        // Act
        String result = ExplanationUtils.describeCombinationTechnique(techniqueName, weights);
        // Assert
        assertEquals("test_technique", result);
    }

    public void testDescribeCombinationTechniqueWithNullTechnique() {
        // Setup
        List<Float> weights = Arrays.asList(1.0f);
        // Act & Assert
        expectThrows(IllegalArgumentException.class, () -> ExplanationUtils.describeCombinationTechnique(null, weights));
    }
}
