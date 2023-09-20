/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.common;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import com.google.common.collect.ImmutableList;

public abstract class BaseSparseEncodingIT extends BaseNeuralSearchIT {

    @SneakyThrows
    @Override
    protected String prepareModel() {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/UploadSparseEncodingModelRequestBody.json").toURI())
        );
        String modelId = uploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    @SneakyThrows
    protected void prepareSparseEncodingIndex(String indexName, List<String> sparseEncodingFieldNames) {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("mappings").startObject("properties");

        for (String fieldName : sparseEncodingFieldNames) {
            xContentBuilder.startObject(fieldName).field("type", "rank_features").endObject();
        }

        xContentBuilder.endObject().endObject().endObject();
        String indexMappings = xContentBuilder.toString();
        createIndexWithConfiguration(indexName, indexMappings, "");
    }

    @SneakyThrows
    protected void addSparseEncodingDoc(String index, String docId, List<String> fieldNames, List<Map<String, Float>> docs) {
        addSparseEncodingDoc(index, docId, fieldNames, docs, Collections.emptyList(), Collections.emptyList());
    }

    @SneakyThrows
    protected void addSparseEncodingDoc(
        String index,
        String docId,
        List<String> fieldNames,
        List<Map<String, Float>> docs,
        List<String> textFieldNames,
        List<String> texts
    ) {
        Request request = new Request("POST", "/" + index + "/_doc/" + docId + "?refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < fieldNames.size(); i++) {
            builder.field(fieldNames.get(i), docs.get(i));
        }

        for (int i = 0; i < textFieldNames.size(); i++) {
            builder.field(textFieldNames.get(i), texts.get(i));
        }
        builder.endObject();

        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.CREATED, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected float computeExpectedScore(String modelId, Map<String, Float> tokenWeightMap, String queryText) {
        Map<String, Float> queryTokens = runSparseModelInference(modelId, queryText);
        return computeExpectedScore(tokenWeightMap, queryTokens);
    }

    protected float computeExpectedScore(Map<String, Float> tokenWeightMap, Map<String, Float> queryTokens) {
        Float score = 0f;
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            if (tokenWeightMap.containsKey(entry.getKey())) {
                score += entry.getValue() * getFeatureFieldCompressedNumber(tokenWeightMap.get(entry.getKey()));
            }
        }
        return score;
    }

    @SneakyThrows
    protected Map<String, Float> runSparseModelInference(String modelId, String queryText) {
        Response inferenceResponse = makeRequest(
            client(),
            "POST",
            String.format(LOCALE, "/_plugins/_ml/models/%s/_predict", modelId),
            null,
            toHttpEntity(String.format(LOCALE, "{\"text_docs\": [\"%s\"]}", queryText)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        Map<String, Object> inferenceResJson = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(inferenceResponse.getEntity()),
            false
        );

        Object inference_results = inferenceResJson.get("inference_results");
        assertTrue(inference_results instanceof List);
        List<Object> inferenceResultsAsMap = (List<Object>) inference_results;
        assertEquals(1, inferenceResultsAsMap.size());
        Map<String, Object> result = (Map<String, Object>) inferenceResultsAsMap.get(0);
        List<Object> output = (List<Object>) result.get("output");
        assertEquals(1, output.size());
        Map<String, Object> map = (Map<String, Object>) output.get(0);
        assertEquals(1, map.size());
        Map<String, Object> dataAsMap = (Map<String, Object>) map.get("dataAsMap");
        return TokenWeightUtil.fetchListOfTokenWeightMap(List.of(dataAsMap)).get(0);
    }

    // rank_features use lucene FeatureField, which will compress the Float number to 16 bit
    // this function simulate the encoding and decoding progress in lucene FeatureField
    protected Float getFeatureFieldCompressedNumber(Float originNumber) {
        int freqBits = Float.floatToIntBits(originNumber);
        freqBits = freqBits >> 15;
        freqBits = ((int) ((float) freqBits)) << 15;
        return Float.intBitsToFloat(freqBits);
    }
}
