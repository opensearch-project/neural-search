/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.e2e;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.NeuralSearchRestTestCase;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.Locale;

public class HybridCollapseIT extends NeuralSearchRestTestCase {

    private final String COLLAPSE_TEST_INDEX = "collapse-test-index";

    private int currentDocNumber = 1;

    public void testCollapse_whenE2E_thenSuccessful() throws IOException, ParseException {
        createBasicCollapseIndex();
        indexCollapseTestDocuments();
        String pipelineName = createNormalizationPipeline("min_max", "arithmetic_mean");

        final String searchRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject("hybrid")
            .startArray("queries")
            .startObject()
            .startObject("match")
            .field("item", "Chocolate Cake")
            .endObject()
            .endObject()
            .startObject()
            .startObject("bool")
            .startObject("must")
            .startObject("match")
            .field("category", "cakes")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .startObject("collapse")
            .field("field", "item")
            .endObject()
            .endObject()
            .toString();
        final Request searchRequest = new Request(
            RestRequest.Method.GET.name(),
            String.format(Locale.ROOT, "%s/_search?search_pipeline=%s", COLLAPSE_TEST_INDEX, pipelineName)
        );
        searchRequest.setJsonEntity(searchRequestBody);
        Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);

        final String responseBody = EntityUtils.toString(searchResponse.getEntity());

        String collapseDuplicate = "Chocolate Cake";
        assertTrue(isCollapseDuplicateRemoved(responseBody, collapseDuplicate));
    }

    private void createBasicCollapseIndex() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("item")
            .field("type", "keyword")
            .endObject()
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .startObject("price")
            .field("type", "float")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapping = mapping.substring(1, mapping.length() - 1);
        Settings settings = Settings.EMPTY;
        createIndex(COLLAPSE_TEST_INDEX, settings, mapping);

        assertTrue(indexExists(COLLAPSE_TEST_INDEX));
    }

    private void indexCollapseTestDocuments() throws IOException {
        indexCollapseDocument("Chocolate Cake", "cakes", 18);
        indexCollapseDocument("Chocolate Cake", "cakes", 15);
        indexCollapseDocument("Vanilla Cake", "cakes", 12);
        indexCollapseDocument("Apple Pie", "pies", 15);
    }

    private void indexCollapseDocument(String item, String category, int price) throws IOException {
        final String indexRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("item", item)
            .field("category", category)
            .field("price", price)
            .endObject()
            .toString();
        final Request indexRequest = new Request(
            RestRequest.Method.POST.name(),
            String.format(Locale.ROOT, "%s/_doc/%d?refresh", COLLAPSE_TEST_INDEX, currentDocNumber)
        );
        indexRequest.setJsonEntity(indexRequestBody);
        assertOK(client().performRequest(indexRequest));
        currentDocNumber++;
    }

    private boolean isCollapseDuplicateRemoved(String responseBody, String collapseDuplicate) {
        // Collapse field should only be present twice in the response body
        // First occurrence should be the hit
        // Second occurrence should be specifying that hit's collapse field

        // Find first occurrence
        int firstIndex = responseBody.indexOf(collapseDuplicate);
        if (firstIndex == -1) return false;

        // Find second occurrence
        int secondIndex = responseBody.indexOf(collapseDuplicate, firstIndex + 1);
        if (secondIndex == -1) return false;

        // Check there isn't a third occurrence
        int thirdIndex = responseBody.indexOf(collapseDuplicate, secondIndex + 1);
        return thirdIndex == -1;
    }
}
