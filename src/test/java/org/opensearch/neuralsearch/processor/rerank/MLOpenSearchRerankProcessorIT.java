/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.TestUtils.DEFAULT_USER_AGENT;

@Log4j2
public class MLOpenSearchRerankProcessorIT extends BaseNeuralSearchIT {

    private final static String PIPELINE_NAME = "rerank-mlos-pipeline";
    private final static String INDEX_NAME = "rerank-test";
    private final static String TEXT_REP_1 = "Jacques loves fish. Fish make Jacques happy";
    private final static String TEXT_REP_2 = "Fish like to eat plankton";
    private final static String INDEX_CONFIG = "{\"mappings\": {\"properties\": {\"text_representation\": {\"type\": \"text\"}}}}";

    @SneakyThrows
    public void testCrossEncoderRerankProcessor() {
        String modelId = null;
        try {
            modelId = uploadTextSimilarityModel();
            loadModel(modelId);
            createSearchPipelineViaConfig(modelId, PIPELINE_NAME, "processor/RerankMLOpenSearchPipelineConfiguration.json");
            setupIndex();
            runQueries();
        } finally {
            wipeOfTestResources(INDEX_NAME, null, modelId, PIPELINE_NAME);
        }
    }

    private String uploadTextSimilarityModel() throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/UploadTextSimilarityModelRequestBody.json").toURI())
        );
        return registerModelGroupAndUploadModel(requestBody);
    }

    private void setupIndex() throws Exception {
        createIndexWithConfiguration(INDEX_NAME, INDEX_CONFIG, PIPELINE_NAME);
        Response response1 = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(String.format(LOCALE, "{\"text_representation\": \"%s\"}", TEXT_REP_1)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Response response2 = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(String.format(LOCALE, "{\"text_representation\": \"%s\"}", TEXT_REP_2)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response1.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
        map = XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response2.getEntity()), false);
        assertEquals("created", map.get("result"));
    }

    private void runQueries() throws Exception {
        Map<String, Object> response1 = search("What do fish eat?");
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> hits = (List<Map<String, ?>>) ((Map<String, ?>) response1.get("hits")).get("hits");
        @SuppressWarnings("unchecked")
        Map<String, String> hit0Source = (Map<String, String>) hits.get(0).get("_source");
        assert ((String) hit0Source.get("text_representation")).equals(TEXT_REP_2);
        @SuppressWarnings("unchecked")
        Map<String, String> hit1Source = (Map<String, String>) hits.get(1).get("_source");
        assert ((String) hit1Source.get("text_representation")).equals(TEXT_REP_1);

        Map<String, Object> response2 = search("Who loves fish?");
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> hits2 = (List<Map<String, ?>>) ((Map<String, ?>) response2.get("hits")).get("hits");
        @SuppressWarnings("unchecked")
        Map<String, String> hit2Source = (Map<String, String>) hits2.get(0).get("_source");
        assert ((String) hit2Source.get("text_representation")).equals(TEXT_REP_1);
        @SuppressWarnings("unchecked")
        Map<String, String> hit3Source = (Map<String, String>) hits2.get(1).get("_source");
        assert ((String) hit3Source.get("text_representation")).equals(TEXT_REP_2);
    }

    private Map<String, Object> search(String queryText) throws Exception {
        String jsonQueryFrame = "{\"query\":{\"match_all\":{}},\"ext\":{\"rerank\":{\"query_context\": {\"query_text\":\"%s\"}}}}";
        String jsonQuery = String.format(LOCALE, jsonQueryFrame, queryText);
        log.info(jsonQuery);
        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        request.addParameter("search_pipeline", PIPELINE_NAME);
        request.setJsonEntity(jsonQuery);

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());

        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }
}
