/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.ml.repackage.com.google.common.net.HttpHeaders.USER_AGENT;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;

@Log4j2
public class ByFieldRerankProcessorIT extends BaseNeuralSearchIT {

    private final static String PIPELINE_NAME = "rerank-byfield-pipeline";
    private final static String INDEX_NAME = "diary_index";
    private final static String INDEX_CONFIG = """
        {
            "mappings" : {
                "properties" : {
                    "diary" : { "type" : "text" },
                    "similarity_score" : { "type" : "float" }
                }
            }
        }
        """.replace("\n", "");
    private final static List<Map.Entry<String, Float>> sampleCrossEncoderData = List.of(
        Map.entry("how are you", -11.055182f),
        Map.entry("today is sunny", 8.969885f),
        Map.entry("today is july fifth", -5.736348f),
        Map.entry("it is winter", -10.045217f)
    );
    private final static String SAMPLE_CROSS_ENCODER_DATA_FORMAT = """
        {
            "diary" : "%s",
            "similarity_score" :  %s
        }
        """.replace("\n", "");

    private final static String PATH_TO_BY_FIELD_RERANK_PIPELINE_TEMPLATE = "processor/ReRankByFieldPipelineConfiguration.json";
    private final static String POST = "POST";
    private final static String TARGET_FIELD = "similarity_score";
    private final static String REMOVE_TARGET_FIELD = "true";
    private final static String KEEP_PREVIOUS_FIELD = "true";
    private SearchResponse searchResponse;

    /**
     * This test creates a simple index with as many documents that
     * {@code sampleCrossEncoderData} has. It will then onboard a search pipeline
     * with the byFieldRerankProcessor. When it applies the search pipeline it will
     * capture the response string into a SearchResponse processor, which is tested like
     * the Unit Tests.
     * <hr>
     * In this scenario the <code>target_field</code> is found within the first level and the
     * <code>target_field</code> will be removed.
     *
     */
    @SneakyThrows
    public void testByFieldRerankProcessor() throws IOException {
        try {
            createAndPopulateIndex();
            createPipeline();
            applyPipeLine();
            testSearchResponse();
        } finally {
            wipeOfTestResources(INDEX_NAME, null, null, PIPELINE_NAME);
        }
    }

    private void createAndPopulateIndex() throws Exception {
        createIndexWithConfiguration(INDEX_NAME, INDEX_CONFIG, PIPELINE_NAME);
        for (int i = 0; i < sampleCrossEncoderData.size(); i++) {
            String diary = sampleCrossEncoderData.get(i).getKey();
            String similarity = sampleCrossEncoderData.get(i).getValue() + "";

            Response responseI = makeRequest(
                client(),
                POST,
                INDEX_NAME + "/_doc?refresh",
                null,
                toHttpEntity(String.format(LOCALE, SAMPLE_CROSS_ENCODER_DATA_FORMAT, diary, similarity)),
                ImmutableList.of(new BasicHeader(USER_AGENT, DEFAULT_USER_AGENT))
            );

            Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(responseI.getEntity()),
                false
            );

            assertEquals("The index has not been `created` instead was " + map.get("result"), "created", map.get("result"));
        }
    }

    private void createPipeline() throws URISyntaxException, IOException, ParseException {
        String pipelineConfiguration = String.format(
            LOCALE,
            Files.readString(Path.of(classLoader.getResource(PATH_TO_BY_FIELD_RERANK_PIPELINE_TEMPLATE).toURI())),
            TARGET_FIELD,
            REMOVE_TARGET_FIELD,
            KEEP_PREVIOUS_FIELD
        ).replace("\"true\"", "true").replace("\"false\"", "false");

        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + PIPELINE_NAME,
            null,
            toHttpEntity(pipelineConfiguration),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("Could not create the pipeline with node:" + node, "true", node.get("acknowledged").toString());
    }

    private void applyPipeLine() throws IOException, ParseException {
        Request request = new Request(POST, "/" + INDEX_NAME + "/_search");
        request.addParameter("search_pipeline", PIPELINE_NAME);
        // Filter out index metaData and only get document data. This gives search hits a score of 1 because of match all
        request.setJsonEntity("""
            {
                "query": {
                   "match_all": {}
                 }
            }
            """.replace("\n", ""));

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        this.searchResponse = stringToSearchResponse(responseBody);
    }

    private void testSearchResponse() {
        List<Map.Entry<String, Float>> sortedDescendingSampleData = sampleCrossEncoderData.stream()
            .sorted(Map.Entry.<String, Float>comparingByKey().reversed())
            .toList();

        SearchHit[] searchHits = this.searchResponse.getHits().getHits();
        assertEquals("The sample data size should match the search response hits", sampleCrossEncoderData.size(), searchHits.length);

        for (int i = 0; i < searchHits.length; i++) {
            float currentSimilarityScore = sortedDescendingSampleData.get(i).getValue();
            String currentDiary = sortedDescendingSampleData.get(i).getKey();
            SearchHit hit = this.searchResponse.getHits().getAt(i);

            assertEquals(
                "The new score at hit[" + i + "] should match the current sampleScore",
                currentSimilarityScore,
                hit.getScore(),
                0.01
            );

            Map<String, Object> sourceMap = hit.getSourceAsMap();
            assertEquals("The source map at hit[" + i + "] should be 2 keys `previous_score` and `diary`", 2, sourceMap.size());

            float previousScore = (((Number) sourceMap.get("previous_score")).floatValue());
            String diary = (String) sourceMap.get("diary");

            assertEquals("The `previous_score` should be 1.0f", 1.0f, previousScore, 0.01);
            assertEquals("The `diary` fields should match based on the score", currentDiary, diary);
        }
    }

    // This assumes that the response is in the shape of a SearchResponse Object
    private SearchResponse stringToSearchResponse(String response) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response);

        return SearchResponse.fromXContent(parser);
    }
}
