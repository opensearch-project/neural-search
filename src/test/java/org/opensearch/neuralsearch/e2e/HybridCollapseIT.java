/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.e2e;

import org.junit.Before;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.sort.SortBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;

public class HybridCollapseIT extends BaseNeuralSearchIT {

    private static final String COLLAPSE_TEST_INDEX = "collapse-test-index";
    private static final String TEST_TEXT_FIELD_ITEM = "item";
    private static final String TEST_TEXT_FIELD_CATEGORY = "category";
    private static final String TEST_FLOAT_FIELD = "price";
    private static final String SEARCH_PIPELINE = "test-pipeline";
    private static final int NUMBER_OF_SHARDS_FIVE = 5;
    private static final int NUMBER_OF_SHARDS_ONE = 1;
    private static final String TEST_KEYWORD_FIELD_AUTHOR = "author";
    private static final String TEST_TEXT_FIELD_ATTACHMENT_DATA = "attachmentData";
    private static final String TEST_VECTOR_FIELD_CHUNK_EMBEDDING = "chunk_embedding";
    private static final String TEST_TEXT_FIELD_DESCRIPTION = "description";
    private static final String TEST_NESTED_FIELD_USER = "user";
    private static final String TEST_INTEGER_FIELD_AGE = "age";
    private static final String DEFAULT_INDEX_CONFIGURATION = "default_config";
    private static final String KNN_INDEX_CONFIGURATION = "knn_config";
    public static final float DELTA_FOR_SCORE_ASSERTION = 0.001f;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createSearchPipeline(SEARCH_PIPELINE, "min_max", "arithmetic_mean", Map.of());
    }

    public void testCollapse_withSingleShard_thenSuccessful() {
        createTestIndexAndIngestDocuments(DEFAULT_INDEX_CONFIGURATION, NUMBER_OF_SHARDS_ONE);
        testCollapse_whenE2E_thenSuccessful();
        testCollapse_whenE2E_andSortEnabled_thenSuccessful();
        testCollapse_whenE2EWithInnerHits_thenSuccessful();

        // For min_score=0.5005f, it filters out 1 doc
        testCollapse_whenE2E_withMinScore_thenSuccessful(0.5005f, 1, 2);
    }

    public void testCollapse_withMultipleShard_thenSuccessful() {
        createTestIndexAndIngestDocuments(DEFAULT_INDEX_CONFIGURATION, NUMBER_OF_SHARDS_FIVE);
        testCollapse_whenE2E_thenSuccessful();
        testCollapse_whenE2E_andSortEnabled_thenSuccessful();
        testCollapse_whenE2EWithInnerHits_thenSuccessful();
        testCollapse_whenShardHasNoDocuments_thenSuccessful();

        // For min_score=0.5005f, it filters out no docs;
        testCollapse_whenE2E_withMinScore_thenSuccessful(0.5005f, 1, 2);
    }

    public void testCollapseOnNestedFieldWithInnerHits_withoutReferenceOnGroup_thenSuccessful() {
        createTestIndexAndIngestDocuments(KNN_INDEX_CONFIGURATION, NUMBER_OF_SHARDS_FIVE);
        // In this tests, it will group all the ages under emily bronte author document. When there is ambiguity opensearch adds all the
        // documents in the same group.
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.termQuery(TEST_KEYWORD_FIELD_AUTHOR, "Emily Brontë"))
            .add(QueryBuilders.matchAllQuery());
        InnerHitBuilder collapsedAgesInnerHitBuilder = new InnerHitBuilder("collapsed_ages");
        collapsedAgesInnerHitBuilder.setSize(100);
        collapsedAgesInnerHitBuilder.setSorts(List.of(SortBuilders.scoreSort()));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(collapsedAgesInnerHitBuilder);
        CollapseContext collapseContext = new CollapseContext("user.age", null, innerHitBuilders);
        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> actualHits = (List<Map<String, Object>>) hits.get("hits");
        assertEquals(1, actualHits.size());
        Map<String, Object> firstHit = actualHits.get(0);
        assertTrue(firstHit.containsKey("inner_hits"));
        Map<String, Object> innerHitsOfDocument = (Map<String, Object>) firstHit.get("inner_hits");
        assertTrue(innerHitsOfDocument.containsKey("collapsed_ages"));
        Map<String, Object> collapsedAges = (Map<String, Object>) innerHitsOfDocument.get("collapsed_ages");
        Map<String, Object> collapsedAgesHits = (Map<String, Object>) collapsedAges.get("hits");
        List<?> collapseInnerHits = (List<?>) collapsedAgesHits.get("hits");
        assertEquals(4, collapseInnerHits.size());
    }

    public void testCollapseWithInnerHits_whenWandsScorerInSearch_thenSuccessful() {
        createTestIndexAndIngestDocuments(KNN_INDEX_CONFIGURATION, NUMBER_OF_SHARDS_ONE);
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(TEST_VECTOR_FIELD_CHUNK_EMBEDDING, new float[] { 0.3f, 0.4f, 0.3f }, 10);
        var hybridQuery = new HybridQueryBuilder().add(
            QueryBuilders.queryStringQuery("storm~1").field(TEST_TEXT_FIELD_DESCRIPTION, 2.0f).field(TEST_TEXT_FIELD_ATTACHMENT_DATA)
        ).add(knnQueryBuilder);
        InnerHitBuilder collapsedAgesInnerHitBuilder = new InnerHitBuilder("authors");
        collapsedAgesInnerHitBuilder.setSize(10);
        collapsedAgesInnerHitBuilder.setSorts(List.of(SortBuilders.scoreSort()));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(collapsedAgesInnerHitBuilder);
        CollapseContext collapseContext = new CollapseContext("author", null, innerHitBuilders);
        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> actualHits = (List<Map<String, Object>>) hits.get("hits");
        // 3 different authors are present in the documents so 3 groups will be there.
        assertEquals(3, actualHits.size());
    }

    public void testCollapseWithInnerHits_whenOneDocumentWithNoCollapseFieldExists_thenSuccessful() {
        createTestIndexAndIngestDocuments(KNN_INDEX_CONFIGURATION, NUMBER_OF_SHARDS_ONE);
        // Index the document with no author field and then apply collapse on author field. This document will be categorised in `null`
        // group.
        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "5",
            List.of(TEST_VECTOR_FIELD_CHUNK_EMBEDDING),
            List.<Object[]>of(new Double[] { 0.8, 0.4, 0.3 }),
            List.of(TEST_TEXT_FIELD_ATTACHMENT_DATA, TEST_TEXT_FIELD_DESCRIPTION),
            List.of("test document with no author", "test document with no author"),
            List.of(TEST_NESTED_FIELD_USER),
            Map.of(TEST_NESTED_FIELD_USER, List.of(Map.of(TEST_INTEGER_FIELD_AGE, "38"))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(TEST_VECTOR_FIELD_CHUNK_EMBEDDING, new float[] { 0.3f, 0.4f, 0.3f }, 10);
        var hybridQuery = new HybridQueryBuilder().add(
            QueryBuilders.queryStringQuery("storm~1").field(TEST_TEXT_FIELD_DESCRIPTION, 2.0f).field(TEST_TEXT_FIELD_ATTACHMENT_DATA)
        ).add(knnQueryBuilder);
        InnerHitBuilder collapsedAgesInnerHitBuilder = new InnerHitBuilder("authors");
        collapsedAgesInnerHitBuilder.setSize(10);
        collapsedAgesInnerHitBuilder.setSorts(List.of(SortBuilders.scoreSort()));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(collapsedAgesInnerHitBuilder);
        CollapseContext collapseContext = new CollapseContext("author", null, innerHitBuilders);
        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            100,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> actualHits = (List<Map<String, Object>>) hits.get("hits");
        // 3 different authors are present in the documents so 3 groups will be there.
        assertEquals(4, actualHits.size());
    }

    private void testCollapse_whenE2E_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_ITEM, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_CATEGORY, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_ITEM, null, null);

        Map<String, Object> searchResponseWithCollapse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponseWithCollapse.toString(), collapseDuplicate));

        Map<String, Object> searchResponseWithoutCollapse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            null
        );
        Map<Object, Double> fieldToHighestScoreMap = getFieldWithHighestScoreMap(searchResponseWithoutCollapse, "item", "keyword");

        Map<String, Double> collapseValueToScoreMap = getCollapseValueWithScoreMap(searchResponseWithCollapse);

        for (Map.Entry<String, Double> entry : collapseValueToScoreMap.entrySet()) {
            String key = entry.getKey();
            Double value = entry.getValue();

            Double scoreFromWithoutCollapse = fieldToHighestScoreMap.get(key);
            assertEquals(value, scoreFromWithoutCollapse, DELTA_FOR_SCORE_ASSERTION);
        }
    }

    private void testCollapse_whenE2E_andSortEnabled_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_ITEM, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_CATEGORY, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_ITEM, null, null);

        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            List.of(SortBuilders.fieldSort(TEST_FLOAT_FIELD)),
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponse.toString(), collapseDuplicate));
        String responseString = searchResponse.toString();
        assertTrue(responseString.indexOf("Vanilla") < responseString.indexOf("Chocolate"));
    }

    private void testCollapse_whenE2EWithInnerHits_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_ITEM, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_CATEGORY, "cakes")));

        InnerHitBuilder cheapestItemsBuilder = new InnerHitBuilder("cheapest_items");
        cheapestItemsBuilder.setSize(2);
        cheapestItemsBuilder.setSorts(List.of(SortBuilders.fieldSort(TEST_FLOAT_FIELD)));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(cheapestItemsBuilder);
        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_ITEM, null, innerHitBuilders);

        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );
        String responseString = searchResponse.toString();
        assertTrue(responseString.contains("cheapest_items"));
    }

    private void testCollapse_whenShardHasNoDocuments_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_ITEM, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_CATEGORY, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_ITEM, null, null);

        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponse.toString(), collapseDuplicate));
    }

    private void testCollapse_whenE2E_withMinScore_thenSuccessful(Float minScore, int expectedCollectedHits, int expectedTotalHits) {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_ITEM, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_CATEGORY, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_ITEM, null, null);

        Map<String, Object> searchResponse = search(
            COLLAPSE_TEST_INDEX,
            hybridQuery,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            collapseContext,
            null,
            minScore
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponse.toString(), collapseDuplicate));

        assertEquals(expectedCollectedHits, getHitCount(searchResponse));
        Map<String, Object> total = getTotalHits(searchResponse);
        assertNotNull(total.get("value"));
        assertEquals(expectedTotalHits, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    private void createTestIndexAndIngestDocuments(String configuration, int numOfShards) {
        String indexConfiguration = getIndexConfiguration(configuration, numOfShards);
        createIndexWithConfiguration(COLLAPSE_TEST_INDEX, indexConfiguration, null);
        assertTrue(indexExists(COLLAPSE_TEST_INDEX));
        indexTestDocuments(configuration);
    }

    @SneakyThrows
    private void indexTestDocuments(String configuration) {
        switch (configuration) {
            case DEFAULT_INDEX_CONFIGURATION:
                indexDocumentsForDefaultConfiguration();
                break;
            case KNN_INDEX_CONFIGURATION:
                indexDocumentsForKNNConfiguration();
                break;
            default:
                throw new IllegalArgumentException("Invalid configuration: " + configuration);
        }

    }

    private void indexDocumentsForDefaultConfiguration() {
        List<String> textFields = List.of(TEST_TEXT_FIELD_ITEM, TEST_TEXT_FIELD_CATEGORY);
        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "1",
            List.of(),
            List.of(),
            textFields,
            List.of("Chocolate Cake", "cakes"),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(TEST_FLOAT_FIELD),
            List.of("18"),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "2",
            List.of(),
            List.of(),
            textFields,
            List.of("Chocolate Cake", "cakes"),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(TEST_FLOAT_FIELD),
            List.of("15"),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "3",
            List.of(),
            List.of(),
            textFields,
            List.of("Vanilla Cake", "cakes"),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(TEST_FLOAT_FIELD),
            List.of("12"),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "4",
            List.of(),
            List.of(),
            textFields,
            List.of("Apple Pie", "pies"),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(TEST_FLOAT_FIELD),
            List.of("15"),
            null
        );
    }

    private void indexDocumentsForKNNConfiguration() {
        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "1",
            List.of(TEST_VECTOR_FIELD_CHUNK_EMBEDDING),
            List.<Object[]>of(new Double[] { 0.8, 0.4, 0.3 }),
            List.of(TEST_TEXT_FIELD_ATTACHMENT_DATA, TEST_TEXT_FIELD_DESCRIPTION),
            List.of(
                "Wuthering Heights, Emily Brontë's 1847 novel, is a dark, passionate tale set on the bleak Yorkshire moors, exploring obsessive love, revenge, and social class through the destructive relationship of Catherine Earnshaw and Heathcliff, framed by a narrative where outsider Mr. Lockwood hears the tragic story from housekeeper Nelly Dean, revealing a world of fierce emotions and supernatural undertones.",
                "Wuthering Heights"
            ),
            List.of(TEST_NESTED_FIELD_USER),
            Map.of(TEST_NESTED_FIELD_USER, List.of(Map.of(TEST_INTEGER_FIELD_AGE, "38"))),
            List.of(),
            List.of(),
            List.of(TEST_KEYWORD_FIELD_AUTHOR),
            List.of("Emily Brontë"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "2",
            List.of(TEST_VECTOR_FIELD_CHUNK_EMBEDDING),
            List.<Object[]>of(new Double[] { 0.5, 0.5, 0.5 }),
            List.of(TEST_TEXT_FIELD_ATTACHMENT_DATA, TEST_TEXT_FIELD_DESCRIPTION),
            List.of(
                "Emily Brontë's 'The Night is Darkening Round Me' (also known as 'Spellbound') is a powerful poem about being trapped by an intense, perhaps loving, force amidst a fierce, darkening natural landscape, using vivid imagery of wild winds, snow, and endless wastes to convey a feeling of being bound by a 'tyrant spell' that, despite its gloom, the speaker welcomes, refusing to leave due to an internal resolve or connection stronger than external dread. The poem sets a scene of impending storm and desolation, but the speaker's repeated insistence, 'I will not, cannot go,' reveals a chosen captivity, highlighting themes of nature, internal feeling, and a powerful, binding emotion. ",
                "The Night is Darkening Round Me"
            ),
            List.of(TEST_NESTED_FIELD_USER),
            Map.of(TEST_NESTED_FIELD_USER, List.of(Map.of(TEST_INTEGER_FIELD_AGE, "36"))),
            List.of(),
            List.of(),
            List.of(TEST_KEYWORD_FIELD_AUTHOR),
            List.of("Emily Brontë"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "3",
            List.of(TEST_VECTOR_FIELD_CHUNK_EMBEDDING),
            List.<Object[]>of(new Double[] { 0.3, 0.4, 0.3 }),
            List.of(TEST_TEXT_FIELD_ATTACHMENT_DATA, TEST_TEXT_FIELD_DESCRIPTION),
            List.of(
                "The Magic Mountain (1924) by Thomas Mann is a monumental novel about young German engineer Hans Castorp, who visits his cousin at a tuberculosis sanatorium in the Swiss Alps, intending a short stay but getting drawn into the isolated, timeless world of illness, philosophy, and pre-WWI European culture for seven years, exploring life, death, love (with Clavdia Cauchat), and politics before being pulled back to the 'flatland' and the outbreak of war. It's a philosophical bildungsroman (coming-of-age story) using the microcosm of the Berghof sanatorium to reflect the macrocosm of a world on the brink of chaos, contrasting health and sickness, spirit and flesh, and intellect versus instinct. ",
                "The Magic Mountain"
            ),
            List.of(TEST_NESTED_FIELD_USER),
            Map.of(TEST_NESTED_FIELD_USER, List.of(Map.of(TEST_INTEGER_FIELD_AGE, "98"))),
            List.of(),
            List.of(),
            List.of(TEST_KEYWORD_FIELD_AUTHOR),
            List.of("Thomas Mann"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );

        indexTheDocument(
            COLLAPSE_TEST_INDEX,
            "4",
            List.of(TEST_VECTOR_FIELD_CHUNK_EMBEDDING),
            List.<Object[]>of(new Double[] { 0.7, 0.4, 0.7 }),
            List.of(TEST_TEXT_FIELD_ATTACHMENT_DATA, TEST_TEXT_FIELD_DESCRIPTION),
            List.of(
                "The Unbearable Lightness of Being's introduction sets up the novel's core philosophical dilemma: the conflict between 'lightness' (meaninglessness, freedom from consequence) and 'weight' (purpose, responsibility, eternal return), using the backdrop of Prague during the 1968 Soviet invasion to explore these ideas through the interwoven lives of surgeon Tomas, his wife Tereza, his mistress Sabina, and her lover Franz, blending love, politics, and existential questions. It immediately contrasts Nietzsche's eternal return (heavy) with Parmenides' concept of single-occurrence life (light), suggesting life's fleeting moments make choices weightless, a tension central to the characters' struggles with love, fidelity, and freedom.",
                "The Unbearable Lightness of Being"
            ),
            List.of(TEST_NESTED_FIELD_USER),
            Map.of(TEST_NESTED_FIELD_USER, List.of(Map.of(TEST_INTEGER_FIELD_AGE, "48"))),
            List.of(),
            List.of(),
            List.of(TEST_KEYWORD_FIELD_AUTHOR),
            List.of("Milan Kundera"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );
    }

    private boolean isCollapseDuplicateRemoved(String responseBody, String collapseDuplicate) {
        int firstIndex = responseBody.indexOf(collapseDuplicate);
        if (firstIndex == -1) return false;

        int secondIndex = responseBody.indexOf(collapseDuplicate, firstIndex + 1);
        if (secondIndex == -1) return false;

        int thirdIndex = responseBody.indexOf(collapseDuplicate, secondIndex + 1);
        return thirdIndex == -1;
    }

    private String getIndexConfiguration(String configuration, int numberOfShards) throws IOException {
        return switch (configuration) {
            case DEFAULT_INDEX_CONFIGURATION -> XContentFactory.jsonBuilder()
                .startObject()
                .startObject("settings")
                .field("number_of_shards", numberOfShards)
                .field("number_of_replicas", 1)
                .endObject()
                .startObject("mappings")
                .startObject("properties")
                .startObject(TEST_TEXT_FIELD_ITEM)
                .field("type", "keyword")
                .endObject()
                .startObject(TEST_TEXT_FIELD_CATEGORY)
                .field("type", "keyword")
                .endObject()
                .startObject(TEST_FLOAT_FIELD)
                .field("type", "float")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .toString();
            case KNN_INDEX_CONFIGURATION -> XContentFactory.jsonBuilder()
                .startObject()
                .startObject("settings")
                .field("number_of_shards", numberOfShards)
                .field("number_of_replicas", 1)
                .field("index.knn", true)
                .endObject()
                .startObject("mappings")
                .startObject("properties")
                .startObject(TEST_KEYWORD_FIELD_AUTHOR)
                .field("type", "keyword")
                .endObject()
                .startObject(TEST_TEXT_FIELD_ATTACHMENT_DATA)
                .field("type", "text")
                .endObject()
                .startObject(TEST_NESTED_FIELD_USER)
                .field("type", "nested")
                .startObject("properties")
                .startObject(TEST_INTEGER_FIELD_AGE)
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
                .startObject(TEST_VECTOR_FIELD_CHUNK_EMBEDDING)
                .field("type", "knn_vector")
                .field("dimension", 3)
                .startObject("method")
                .field("name", "hnsw")
                .field("space_type", "l2")
                .field("engine", "lucene")
                .startObject("parameters")
                .endObject()
                .endObject()
                .endObject()
                .startObject(TEST_TEXT_FIELD_DESCRIPTION)
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .toString();
            default -> throw new IllegalStateException("Unexpected value: " + configuration);
        };
    }

    private Map<String, Double> getCollapseValueWithScoreMap(Map<String, Object> collapseResponse) {
        Map<String, Double> collapseValueToScoreMap = new HashMap<>();
        Map<String, Object> hits = (Map<String, Object>) collapseResponse.get("hits");
        List<Map<String, Object>> actualHits = (List<Map<String, Object>>) hits.get("hits");
        for (Map<String, Object> actualHit : actualHits) {
            Double score = (Double) actualHit.get("_score");
            Map<String, Object> fields = (Map<String, Object>) actualHit.get("fields");
            ArrayList<Object> items = (ArrayList<Object>) fields.get("item");
            for (Object item : items) {
                String collapsedValue = item.toString();
                collapseValueToScoreMap.put(collapsedValue, score);
            }
        }
        return collapseValueToScoreMap;
    }

    private Map<Object, Double> getFieldWithHighestScoreMap(
        Map<String, Object> searchResponseWithoutCollapse,
        String fieldName,
        String fieldType
    ) {
        Map<Object, Double> fieldWithHighestScoreMap = new HashMap<>();
        Map<String, Object> hits = (Map<String, Object>) searchResponseWithoutCollapse.get("hits");
        List<Map<String, Object>> actualHits = (List<Map<String, Object>>) hits.get("hits");
        for (Map<String, Object> actualHit : actualHits) {
            Map<String, Object> source = (Map<String, Object>) actualHit.get("_source");
            Double score = (Double) actualHit.get("_score");
            if (fieldType.equals("keyword")) {
                String fieldValue = source.get(fieldName).toString();
                fieldWithHighestScoreMap.put(fieldValue, score);
            } else if (fieldType.equals("numeric")) {
                Long fieldValue = (Long) source.get(fieldName);
                fieldWithHighestScoreMap.put(fieldValue, score);
            } else {
                throw new IllegalStateException("Unexpected field type: " + fieldType);
            }
        }
        return fieldWithHighestScoreMap;
    }
}
