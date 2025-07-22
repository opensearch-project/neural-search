/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.e2e;

import org.junit.Before;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.sort.SortBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_COLLAPSE_DOCS_PER_GROUP;

public class HybridCollapseIT extends BaseNeuralSearchIT {

    private static final String COLLAPSE_TEST_INDEX = "collapse-test-index";
    private static final String TEST_TEXT_FIELD_1 = "item";
    private static final String TEST_TEXT_FIELD_2 = "category";
    private static final String TEST_FLOAT_FIELD = "price";
    private static final String SEARCH_PIPELINE = "test-pipeline";
    private static final int DOCS_PER_GROUP_PER_SUBQUERY = 5;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createTestIndex();
        indexTestDocuments();
        createSearchPipeline(SEARCH_PIPELINE, "min_max", "arithmetic_mean", Map.of());
    }

    public void testCollapse_whenE2E_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_1, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_2, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_1, null, null);

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
            collapseContext
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponse.toString(), collapseDuplicate));
    }

    public void testCollapse_whenE2E_andSortEnabled_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_1, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_2, "cakes")));

        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_1, null, null);

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
            collapseContext
        );

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(searchResponse.toString(), collapseDuplicate));
        String responseString = searchResponse.toString();
        assertTrue(responseString.indexOf("Vanilla") < responseString.indexOf("Chocolate"));
    }

    public void testCollapse_whenE2EWithInnerHits_thenSuccessful() {
        var hybridQuery = new HybridQueryBuilder().add(QueryBuilders.matchQuery(TEST_TEXT_FIELD_1, "Chocolate Cake"))
            .add(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(TEST_TEXT_FIELD_2, "cakes")));

        InnerHitBuilder cheapestItemsBuilder = new InnerHitBuilder("cheapest_items");
        cheapestItemsBuilder.setSize(2);
        cheapestItemsBuilder.setSorts(List.of(SortBuilders.fieldSort(TEST_FLOAT_FIELD)));

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        innerHitBuilders.add(cheapestItemsBuilder);
        CollapseContext collapseContext = new CollapseContext(TEST_TEXT_FIELD_1, null, innerHitBuilders);

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
            collapseContext
        );
        String responseString = searchResponse.toString();
        assertTrue(responseString.contains("cheapest_items"));
    }

    @SneakyThrows
    private void createTestIndex() {
        String indexConfiguration = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field(HYBRID_COLLAPSE_DOCS_PER_GROUP.getKey(), DOCS_PER_GROUP_PER_SUBQUERY)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(TEST_TEXT_FIELD_1)
            .field("type", "keyword")
            .endObject()
            .startObject(TEST_TEXT_FIELD_2)
            .field("type", "keyword")
            .endObject()
            .startObject(TEST_FLOAT_FIELD)
            .field("type", "float")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        createIndexWithConfiguration(COLLAPSE_TEST_INDEX, indexConfiguration, null);
        assertTrue(indexExists(COLLAPSE_TEST_INDEX));
    }

    @SneakyThrows
    private void indexTestDocuments() {
        List<String> textFields = List.of(TEST_TEXT_FIELD_1, TEST_TEXT_FIELD_2);
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

    private boolean isCollapseDuplicateRemoved(String responseBody, String collapseDuplicate) {
        int firstIndex = responseBody.indexOf(collapseDuplicate);
        if (firstIndex == -1) return false;

        int secondIndex = responseBody.indexOf(collapseDuplicate, firstIndex + 1);
        if (secondIndex == -1) return false;

        int thirdIndex = responseBody.indexOf(collapseDuplicate, secondIndex + 1);
        return thirdIndex == -1;
    }
}
