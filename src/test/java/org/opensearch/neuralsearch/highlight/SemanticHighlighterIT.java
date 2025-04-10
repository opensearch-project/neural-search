/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;

public class SemanticHighlighterIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-semantic-highlight-index";
    private static final String TEST_QUERY_TEXT = "artificial intelligence";
    private static final String TEST_KNN_VECTOR_FIELD_NAME = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD_NAME = "test-text-field";
    private static final String TEST_CONTENT = "Machine learning is a field of artificial intelligence that uses statistical techniques. "
        + "Natural language processing is a branch of artificial intelligence that helps computers understand human language. "
        + "Deep learning is a subset of machine learning that uses neural networks with many layers.";
    private final float[] testVector = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        initializeTestIndex();
    }

    /**
     * Helper method to initialize test index with required data
     */
    @SneakyThrows
    private void initializeTestIndex() {
        if (!indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME, TEST_DIMENSION, TEST_SPACE_TYPE))
            );

            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME),
                Collections.singletonList(Floats.asList(testVector).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME),
                Collections.singletonList(TEST_CONTENT)
            );
        }
    }

    /**
     * Tests semantic highlighting with different query types:
     * 1. Match Query
     * 2. Term Query
     * 3. Boolean Query
     * 4. Query String Query
     * 5. Neural Query
     * 6. Hybrid Query
     */
    public void testQueriesWithSemanticHighlighter() throws Exception {
        // Set up models for the test
        String textEmbeddingModelId = prepareModel();
        String sentenceHighlightingModelId = prepareSentenceHighlightingModel();

        // 1. Test Match Query
        /*
         * Query example:
         * {
         *     "query": {
         *         "match": {
         *             "test-text-field": "artificial intelligence"
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        MatchQueryBuilder matchQuery = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, TEST_QUERY_TEXT);
        Map<String, Object> searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                matchQuery,
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, TEST_QUERY_TEXT);

        // 2. Test Term Query
        /*
         *  example:
         * {
         *     "query": {
         *         "term": {
         *             "test-text-field": "intelligence"
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        TermQueryBuilder termQuery = new TermQueryBuilder(TEST_TEXT_FIELD_NAME, "intelligence");
        searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                termQuery,
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, "intelligence");

        // 3. Test Boolean Query
        /*
         *  example:
         * {
         *     "query": {
         *         "bool": {
         *             "must": [
         *                 { "match": { "test-text-field": "artificial" } },
         *                 { "match": { "test-text-field": "intelligence" } }
         *             ],
         *             "should": [
         *                 { "match": { "test-text-field": "learning" } }
         *             ]
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
                .must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "artificial"))
                .must(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "intelligence"))
                .should(new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, "learning"));
        searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                boolQuery,
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, "artificial intelligence");

        // 4. Test Query String Query
        /*
         *  example:
         * {
         *     "query": {
         *         "query_string": {
         *             "query": "artificial AND intelligence",
         *             "default_field": "test-text-field"
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        QueryStringQueryBuilder queryString = new QueryStringQueryBuilder("artificial AND intelligence")
                .defaultField(TEST_TEXT_FIELD_NAME);
        searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                queryString,
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, "artificial intelligence");

        // 5. Test Neural Query
        /*
         *  example:
         * {
         *     "query": {
         *         "neural": {
         *             "test-knn-vector-1": {
         *                 "query_text": "artificial intelligence",
         *                 "model_id": "text-embedding-model-id",
         *                 "k": 1
         *             }
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
                .fieldName(TEST_KNN_VECTOR_FIELD_NAME)
                .queryText(TEST_QUERY_TEXT)
                .modelId(textEmbeddingModelId)
                .k(1)
                .build();
        searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                neuralQueryBuilder,
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, TEST_QUERY_TEXT);

        // 6. Test Hybrid Query
        /*
         *  example:
         * {
         *     "query": {
         *         "hybrid": {
         *             "queries": [
         *                 {
         *                     "match": {
         *                         "test-text-field": "artificial intelligence"
         *                     }
         *                 },
         *                 {
         *                     "neural": {
         *                         "test-knn-vector-1": {
         *                             "query_text": "AI applications",
         *                             "model_id": "text-embedding-model-id"
         *                         }
         *                     }
         *                 }
         *             ]
         *         }
         *     },
         *     "highlight": {
         *         "fields": {
         *             "test-text-field": { "type": "semantic" }
         *         },
         *         "options": {
         *             "model_id": "sentence-highlighting-model-id"
         *         }
         *     }
         * }
         */
        MatchQueryBuilder hybridMatchQuery = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME, TEST_QUERY_TEXT);
        NeuralQueryBuilder hybridNeuralQuery = NeuralQueryBuilder.builder()
                .fieldName(TEST_KNN_VECTOR_FIELD_NAME)
                .queryText("AI applications")
                .modelId(textEmbeddingModelId)
                .k(1)
                .build();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(
                new BoolQueryBuilder().should(hybridMatchQuery).should(hybridNeuralQuery)
        );
        searchResponse = searchWithSemanticHighlighter(
                TEST_BASIC_INDEX_NAME,
                searchSourceBuilder.query(),
                10,
                TEST_TEXT_FIELD_NAME,
                sentenceHighlightingModelId
        );
        verifyHighlightResults(searchResponse, "artificial intelligence");
    }

    private void verifyHighlightResults(Map<String, Object> searchResponse, String expectedContent) {
        Map<String, Object> firstHit = getFirstInnerHit(searchResponse);
        assertNotNull("Search response should contain hits", firstHit);

        @SuppressWarnings("unchecked")
        Map<String, Object> highlight = (Map<String, Object>) firstHit.get("highlight");
        assertNotNull("Hit should contain highlight section", highlight);

        @SuppressWarnings("unchecked")
        List<String> highlightedFields = (List<String>) highlight.get(TEST_TEXT_FIELD_NAME);
        assertNotNull("Highlight should contain the requested field", highlightedFields);
        assertFalse("Highlighted fields should not be empty", highlightedFields.isEmpty());

        String highlightedText = highlightedFields.getFirst();

        // Split the expected content into individual terms
        String[] expectedTerms = expectedContent.toLowerCase(Locale.ROOT).split("\\s+");

        // Check if the highlighted text contains semantically relevant content
        boolean hasRelevantContent = false;
        for (String term : expectedTerms) {
            if (highlightedText.toLowerCase(Locale.ROOT).contains(term)) {
                hasRelevantContent = true;
                break;
            }
        }

        assertTrue("Highlighted text should contain semantically relevant content for query: " + expectedContent, hasRelevantContent);

        // Verify the highlight tags are present
        assertTrue(
            "Highlighted text should contain proper highlight tags",
            highlightedText.contains("<em>") && highlightedText.contains("</em>")
        );
    }
}
