/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getValueFromSource;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.mappingExistsInSource;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.validateRerankCriteria;

public class ProcessorUtilsTests extends OpenSearchTestCase {

    private Map<String, Object> sourceMap;
    private SearchResponse searchResponse;
    private float expectedScore;

    /**
     * SourceMap of the form
     * <pre>
     *  {
     *    "my_field" : "test_value",
     *    "ml": {
     *         "model" : "myModel",
     *         "info"  : {
     *                   "score": 0.95
     *         }
     *    }
     * }
     * </pre>
     */
    public void setUpValidSourceMap() {
        expectedScore = 0.95f;

        sourceMap = new HashMap<>();
        sourceMap.put("my_field", "test_value");

        Map<String, Object> mlMap = new HashMap<>();
        mlMap.put("model", "myModel");

        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("score", expectedScore);

        mlMap.put("info", infoMap);
        sourceMap.put("ml", mlMap);
    }

    /**
     * This creates a search response with two hits, the first hit being in the correct form.
     * While, the second search hit has a non-numeric target field mapping.
     */
    private void setUpInvalidSearchResultsWithTargetFieldHavingNonNumericMapping() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":777}"));
        hits[0].score(1.0F);

        Map<String, DocumentField> dummyMap = new HashMap<>();
        dummyMap.put("test", new DocumentField("test", Collections.singletonList("test-field-mapping")));
        hits[1] = new SearchHit(1, "2", dummyMap, Collections.emptyMap());
        hits[1].sourceRef(new BytesArray("{\"diary\" : \"how do you do\",\"similarity_score\":\"hello world\"}"));
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.searchResponse = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * This creates a search response with two hits, Both are in correct form
     */
    private void setUpValidSearchResults() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":777}"));
        hits[0].score(1.0F);

        hits[1] = new SearchHit(1, "2", Collections.emptyMap(), Collections.emptyMap());
        hits[1].sourceRef(new BytesArray("{\"diary\" : \"how do you do\",\"similarity_score\":888}"));
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.searchResponse = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    public void testValidateRerankCriteria_throwsException_OnSearchResponseHavingNonNumericalScore() {
        String targetField = "similarity_score";
        setUpInvalidSearchResultsWithTargetFieldHavingNonNumericMapping();
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        // Check that the mapping has non-numerical mapping
        ProcessorUtils.SearchHitValidator searchHitValidator = (hit) -> {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Optional<Object> val = getValueFromSource(sourceAsMap, targetField);

            if (!(val.get() instanceof Number)) {
                throw new IllegalArgumentException(
                    "The field mapping to rerank [" + targetField + ": " + val.get() + "] is a not Numerical"
                );
            }

        };

        boolean validRerankCriteria = validateRerankCriteria(searchResponse.getHits().getHits(), searchHitValidator, listener);

        assertFalse("This search response has invalid reranking criteria", validRerankCriteria);

        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argumentCaptor.capture());

        assertEquals(
            "The field mapping to rerank [" + targetField + ": hello world] is a not Numerical",
            argumentCaptor.getValue().getMessage()
        );
        assert (argumentCaptor.getValue() instanceof IllegalArgumentException);
    }

    public void testValidateRerankCriteria_returnTrue_OnSearchResponseHavingCorrectForm() {
        String targetField = "similarity_score";
        setUpValidSearchResults();
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        // This check is emulating the byField SearchHit Validation
        ProcessorUtils.SearchHitValidator searchHitValidator = (hit) -> {
            if (!hit.hasSource()) {
                throw new IllegalArgumentException("There is no source field to be able to perform rerank on hit [" + hit.docId() + "]");
            }

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (!mappingExistsInSource(sourceAsMap, targetField)) {
                throw new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + hit.docId() + "]");
            }
            Optional<Object> val = getValueFromSource(sourceAsMap, targetField);

            if (!(val.get() instanceof Number)) {
                throw new IllegalArgumentException(
                    "The field mapping to rerank [" + targetField + ": " + val.get() + "] is a not Numerical"
                );
            }

        };

        boolean validRerankCriteria = validateRerankCriteria(searchResponse.getHits().getHits(), searchHitValidator, listener);

        assertTrue("This search response has valid reranking criteria", validRerankCriteria);
    }

    public void testGetValueFromSource_returnsExpectedScore_WithExistingKeys() {
        String targetField = "ml.info.score";
        setUpValidSourceMap();
        Optional<Object> result = getValueFromSource(sourceMap, targetField);
        assertTrue(result.isPresent());
        assertEquals(expectedScore, (Float) result.get(), 0.01);
    }

    public void testGetScoreFromSource_returnsExpectedScore_WithExistingKeys() {
        String targetField = "ml.info.score";
        setUpValidSourceMap();
        float result = ProcessorUtils.getScoreFromSourceMap(sourceMap, targetField);
        assertEquals(expectedScore, result, 0.01);
    }

    public void testGetValueFromSource_returnsEmptyValue_WithNonExistingKeys() {
        String targetField = "ml.info.score.wrong";
        setUpValidSourceMap();
        Optional<Object> result = getValueFromSource(sourceMap, targetField);
        assertTrue(result.isEmpty());
    }

    public void testMappingExistsInSource_returnsTrue_withExistingKeys() {
        String targetField = "ml.info.score";
        setUpValidSourceMap();
        boolean result = mappingExistsInSource(sourceMap, targetField);
        assertTrue(result);
    }

    public void testMappingExistsInSource_returnsFalse_withNonExistingKeys() {
        String targetField = "ml.info.score.wrong";
        setUpValidSourceMap();
        boolean result = mappingExistsInSource(sourceMap, targetField);
        assertFalse(result);
    }

    public void testRemoveTargetFieldFromSource_successfullyDeletesTargetField_WithExistingKeys() {
        String targetField = "ml.info.score";
        setUpValidSourceMap();
        ProcessorUtils.removeTargetFieldFromSource(sourceMap, targetField);
        assertEquals("The first level of the map is the containing `my_field` and `ml`", 2, sourceMap.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> innerMLMap = (Map<String, Object>) sourceMap.get("ml");

        assertEquals("The ml map now only has 1 mapping `model` instead of 2", 1, innerMLMap.size());
        assertTrue("The ml map has `model` as a mapping", innerMLMap.containsKey("model"));
        assertFalse("The ml map no longer has the score `info` mapping ", innerMLMap.containsKey("info"));
    }

}
