/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class RRFSearchIT extends BaseNeuralSearchIT {

    private int currentDoc = 1;
    private static final String RRF_INDEX_NAME = "rrf-index";
    private static final String RRF_SEARCH_PIPELINE = "rrf-search-pipeline";

    @SneakyThrows
    public void testRRF() {
        String modelId = prepareModel();
        String ingestPipelineName = "rrf-ingest-pipeline";
        createPipelineProcessor(modelId, ingestPipelineName, ProcessorType.TEXT_EMBEDDING);
        Settings indexSettings = Settings.builder().put("index.knn", true).put("default_pipeline", ingestPipelineName).build();
        String indexMappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "text")
            .endObject()
            .startObject("passage_embedding")
            .field("type", "knn_vector")
            .field("dimension", "768")
            .startObject("method")
            .field("engine", "lucene")
            .field("space_type", "l2")
            .field("name", "hnsw")
            .endObject()
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        // Removes the {} around the string, since they are already included with createIndex
        indexMappings = indexMappings.substring(1, indexMappings.length() - 1);
        String indexName = "rrf-index";
        createIndex(indexName, indexSettings, indexMappings, null);
        addRRFDocuments();
        createDefaultRRFSearchPipeline();

        Map<String, Object> results = searchRRF(modelId);
        Map<String, Object> hits = (Map<String, Object>) results.get("hits");
        ArrayList<HashMap<String, Object>> hitsList = (ArrayList<HashMap<String, Object>>) hits.get("hits");
        assertEquals(3, hitsList.size());
        assertEquals(0.016393442, (Double) hitsList.getFirst().get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.016129032, (Double) hitsList.get(1).get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.015873017, (Double) hitsList.getLast().get("_score"), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    private void addRRFDocuments() {
        addRRFDocument(
            "A West Virginia university women 's basketball team , officials , and a small gathering of fans are in a West Virginia arena .",
            "4319130149.jpg"
        );
        addRRFDocument("A wild animal races across an uncut field with a minimal amount of trees .", "1775029934.jpg");
        addRRFDocument(
            "People line the stands which advertise Freemont 's orthopedics , a cowboy rides a light brown bucking bronco .",
            "2664027527.jpg"
        );
        addRRFDocument("A man who is riding a wild horse in the rodeo is very near to falling off .", "4427058951.jpg");
        addRRFDocument("A rodeo cowboy , wearing a cowboy hat , is being thrown off of a wild white horse .", "2691147709.jpg");
    }

    @SneakyThrows
    private void addRRFDocument(String description, String imageText) {
        addDocument(RRF_INDEX_NAME, String.valueOf(currentDoc++), "text", description, "image_text", imageText);
    }

    @SneakyThrows
    private void createDefaultRRFSearchPipeline() {
        String requestBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("description", "Post processor for hybrid search")
            .startArray("phase_results_processors")
            .startObject()
            .startObject("score-ranker-processor")
            .startObject("combination")
            .field("technique", "rrf")
            .startObject("parameters")
            .field("rank_constant", 60)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();

        makeRequest(
            client(),
            "PUT",
            String.format(LOCALE, "/_search/pipeline/%s", RRF_SEARCH_PIPELINE),
            null,
            toHttpEntity(String.format(LOCALE, requestBody)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SneakyThrows
    private Map<String, Object> searchRRF(String modelId) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_source")
            .startArray("exclude")
            .value("passage_embedding")
            .endArray()
            .endObject()
            .startObject("query")
            .startObject("hybrid")
            .startArray("queries")
            .startObject()
            .startObject("match")
            .startObject("text")
            .field("query", "cowboy rodeo bronco")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("neural")
            .startObject("passage_embedding")
            .field("query_text", "wild west")
            .field("model_id", modelId)
            .field("k", 5)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("GET", "/" + RRF_INDEX_NAME + "/_search?timeout=1000s&search_pipeline=" + RRF_SEARCH_PIPELINE);
        logger.info("Sorting request  " + builder);
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        logger.info("Response  " + responseBody);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }
}
