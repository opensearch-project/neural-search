/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

/**
 * Integration tests for the semantic highlighter query enricher search request processor.
 *
 * <p>The processor lets an operator configure the highlighter {@code model_id} once in a search
 * pipeline instead of repeating it in every query body, so each test issues a query that omits
 * {@code model_id} and asserts that semantic highlighting still resolves a model.
 */
@Log4j2
public class SemanticHighlighterQueryEnricherProcessorIT extends BaseSemanticHighlightingIT {

    private static final String TEST_INDEX = "test-semantic-highlight-enricher-index";
    private static final String SEARCH_PIPELINE = "semantic-highlight-enricher-pipeline";
    private static final String QUERY_TEXT = "treatments for neurodegenerative diseases";
    private static final String INVALID_MODEL_ID = "this-model-does-not-exist";

    private String highlightModelId;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        highlightModelId = prepareSentenceHighlightingModel();
        log.info("Prepared local highlighting model, model ID: {}", highlightModelId);
        prepareHighlightingIndex(TEST_INDEX);
        indexTestDocuments(TEST_INDEX);
    }

    /**
     * The query omits model_id entirely, the pipeline's default_model_id supplies it.
     * Also asserts the processor's own event stat is recorded.
     */
    @SneakyThrows
    public void testQueryEnricher_whenModelIdOmitted_thenDefaultModelIdApplied() {
        enableStats();
        createEnricherPipeline(defaultModelIdConfig(highlightModelId));

        Map<String, Object> searchResponse = search(semanticHighlightQuery(null));

        assertSemanticHighlighting(searchResponse, TEST_FIELD, "treatments");

        Map<String, Object> stats = parseAggregatedNodeStatsResponse(executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>()));
        int enricherExecutions = (int) getNestedValue(stats, EventStatName.SEMANTIC_HIGHLIGHTING_QUERY_ENRICHER_EXECUTIONS);
        assertEquals("Query enricher should have run exactly once", 1, enricherExecutions);
    }

    /** A per field override supplies the model_id for the highlighted field. */
    @SneakyThrows
    public void testQueryEnricher_whenFieldDefaultIdConfigured_thenFieldModelIdApplied() {
        createEnricherPipeline(fieldDefaultIdConfig(TEST_FIELD, highlightModelId));

        Map<String, Object> searchResponse = search(semanticHighlightQuery(null));

        assertSemanticHighlighting(searchResponse, TEST_FIELD, "treatments");
    }

    /**
     * A model_id in the query body must win over the pipeline default. The pipeline is configured
     * with a model that does not exist, so highlighting can only succeed if the query value was kept.
     */
    @SneakyThrows
    public void testQueryEnricher_whenQuerySuppliesModelId_thenPipelineDefaultNotApplied() {
        createEnricherPipeline(defaultModelIdConfig(INVALID_MODEL_ID));

        Map<String, Object> searchResponse = search(semanticHighlightQuery(highlightModelId));

        assertSemanticHighlighting(searchResponse, TEST_FIELD, "treatments");
    }

    /**
     * A non semantic highlighter must be left alone. The pipeline default points at a model that
     * does not exist, so the request only succeeds if the processor did not enrich it.
     */
    @SneakyThrows
    public void testQueryEnricher_whenHighlighterIsNotSemantic_thenNotEnriched() {
        createEnricherPipeline(defaultModelIdConfig(INVALID_MODEL_ID));

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, QUERY_TEXT)
            .endObject()
            .endObject()
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "unified")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> searchResponse = search(searchBody);

        // Unified highlighting still produces <em> fragments, the point is that no model was resolved
        assertSemanticHighlighting(searchResponse, TEST_FIELD, "treatments");
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /**
     * Builds a match query highlighted with {@code type: semantic}, optionally carrying an
     * explicit {@code model_id} in the highlight options.
     */
    @SneakyThrows
    private XContentBuilder semanticHighlightQuery(String modelId) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, QUERY_TEXT)
            .endObject()
            .endObject()
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", SemanticHighlightingConstants.HIGHLIGHTER_TYPE)
            .endObject()
            .endObject();
        if (modelId != null) {
            builder.startObject("options").field(SemanticHighlightingConstants.MODEL_ID, modelId).endObject();
        }
        return builder.endObject().endObject();
    }

    @SneakyThrows
    private String defaultModelIdConfig(String modelId) {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray("request_processors")
            .startObject()
            .startObject(SemanticHighlightingConstants.QUERY_ENRICHER_TYPE)
            .field("default_model_id", modelId)
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
    }

    @SneakyThrows
    private String fieldDefaultIdConfig(String fieldName, String modelId) {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray("request_processors")
            .startObject()
            .startObject(SemanticHighlightingConstants.QUERY_ENRICHER_TYPE)
            .startObject("semantic_highlighter_field_default_id")
            .field(fieldName, modelId)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
    }

    /** Creates the search pipeline and makes it the index default so plain searches pick it up. */
    @SneakyThrows
    private void createEnricherPipeline(String pipelineConfig) {
        Request request = new Request("PUT", "/_search/pipeline/" + SEARCH_PIPELINE);
        request.setJsonEntity(pipelineConfig);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        updateIndexSettings(TEST_INDEX, Settings.builder().put("index.search.default_pipeline", SEARCH_PIPELINE));
    }

    @SneakyThrows
    private Map<String, Object> search(XContentBuilder searchBody) {
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }
}
