/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.common;

import static org.opensearch.neuralsearch.common.VectorUtil.vectorAsListToArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.OpenSearchSecureRestTestCase;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class BaseNeuralSearchIT extends OpenSearchSecureRestTestCase {

    protected static final Locale LOCALE = Locale.ROOT;

    private static final int MAX_TASK_RESULT_QUERY_TIME_IN_SECOND = 60 * 5;

    private static final int DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND = 1000;
    protected static final String DEFAULT_USER_AGENT = "Kibana";
    protected static final String DEFAULT_NORMALIZATION_METHOD = "min_max";
    protected static final String DEFAULT_COMBINATION_METHOD = "arithmetic_mean";
    protected static final String PARAM_NAME_WEIGHTS = "weights";

    protected static final Map<ProcessorType, String> PIPELINE_CONFIGS_BY_TYPE = Map.of(
        ProcessorType.TEXT_EMBEDDING,
        "processor/PipelineConfiguration.json",
        ProcessorType.SPARSE_ENCODING,
        "processor/SparseEncodingPipelineConfiguration.json",
        ProcessorType.TEXT_IMAGE_EMBEDDING,
        "processor/PipelineForTextImageEmbeddingProcessorConfiguration.json"
    );

    protected final ClassLoader classLoader = this.getClass().getClassLoader();

    protected ThreadPool threadPool;
    protected ClusterService clusterService;

    @Before
    public void setupSettings() {
        threadPool = setUpThreadPool();
        clusterService = createClusterService(threadPool);
        if (isUpdateClusterSettings()) {
            updateClusterSettings();
        }
        NeuralSearchClusterUtil.instance().initialize(clusterService);
    }

    protected ThreadPool setUpThreadPool() {
        return new TestThreadPool(getClass().getName(), threadPoolSettings());
    }

    public Settings threadPoolSettings() {
        return Settings.EMPTY;
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        return ClusterServiceUtils.createClusterService(threadPool);
    }

    protected void updateClusterSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        // default threshold for native circuit breaker is 90, it may be not enough on test runner machine
        updateClusterSettings("plugins.ml_commons.native_memory_threshold", 100);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
    }

    @SneakyThrows
    protected void updateClusterSettings(String settingKey, Object value) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field(settingKey, value)
            .endObject()
            .endObject();
        Response response = makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            toHttpEntity(builder.toString()),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected String uploadModel(String requestBody) throws Exception {
        String modelGroupId = registerModelGroup();
        // model group id is dynamically generated, we need to update model update request body after group is registered
        requestBody = requestBody.replace("<MODEL_GROUP_ID>", modelGroupId);

        Response uploadResponse = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_upload",
            null,
            toHttpEntity(requestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> uploadResJson = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(uploadResponse.getEntity()),
            false
        );
        String taskId = uploadResJson.get("task_id").toString();
        assertNotNull(taskId);

        Map<String, Object> taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        for (int i = 0; !isComplete && i < MAX_TASK_RESULT_QUERY_TIME_IN_SECOND; i++) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
            Thread.sleep(DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND);
        }
        String modelId = Optional.ofNullable(taskQueryResult.get("model_id")).map(Object::toString).orElse(null);
        assertNotNull(modelId);
        return modelId;
    }

    protected void loadModel(String modelId) throws Exception {
        Response uploadResponse = makeRequest(
            client(),
            "POST",
            String.format(LOCALE, "/_plugins/_ml/models/%s/_deploy", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> uploadResJson = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(uploadResponse.getEntity()),
            false
        );
        String taskId = uploadResJson.get("task_id").toString();
        assertNotNull(taskId);

        Map<String, Object> taskQueryResult = getTaskQueryResponse(taskId);
        boolean isComplete = checkComplete(taskQueryResult);
        for (int i = 0; !isComplete && i < MAX_TASK_RESULT_QUERY_TIME_IN_SECOND; i++) {
            taskQueryResult = getTaskQueryResponse(taskId);
            isComplete = checkComplete(taskQueryResult);
            Thread.sleep(DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND);
        }
    }

    /**
     * Upload default model and load into the cluster
     *
     * @return modelID
     */
    @SneakyThrows
    protected String prepareModel() {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        String modelId = uploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    /**
     * Execute model inference on the provided query text
     *
     * @param modelId id of model to run inference
     * @param queryText text to be transformed to a model
     * @return text embedding
     */
    @SuppressWarnings("unchecked")
    @SneakyThrows
    protected float[] runInference(String modelId, String queryText) {
        Response inferenceResponse = makeRequest(
            client(),
            "POST",
            String.format(LOCALE, "/_plugins/_ml/_predict/text_embedding/%s", modelId),
            null,
            toHttpEntity(String.format(LOCALE, "{\"text_docs\": [\"%s\"],\"target_response\": [\"sentence_embedding\"]}", queryText)),
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
        List<Float> data = ((List<Double>) map.get("data")).stream().map(Double::floatValue).collect(Collectors.toList());
        return vectorAsListToArray(data);
    }

    protected void createIndexWithConfiguration(String indexName, String indexConfiguration, String pipelineName) throws Exception {
        if (StringUtils.isNotBlank(pipelineName)) {
            indexConfiguration = String.format(LOCALE, indexConfiguration, pipelineName);
        }
        Response response = makeRequest(
            client(),
            "PUT",
            indexName,
            null,
            toHttpEntity(indexConfiguration),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
        assertEquals(indexName, node.get("index").toString());
    }

    protected void createPipelineProcessor(String modelId, String pipelineName) throws Exception {
        createPipelineProcessor(modelId, pipelineName, ProcessorType.TEXT_EMBEDDING);
    }

    protected void createPipelineProcessor(String modelId, String pipelineName, ProcessorType processorType) throws Exception {
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_ingest/pipeline/" + pipelineName,
            null,
            toHttpEntity(
                String.format(
                    LOCALE,
                    Files.readString(Path.of(classLoader.getResource(PIPELINE_CONFIGS_BY_TYPE.get(processorType)).toURI())),
                    modelId
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    protected void createSearchRequestProcessor(String modelId, String pipelineName) throws Exception {
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity(
                String.format(
                    LOCALE,
                    Files.readString(Path.of(classLoader.getResource("processor/SearchRequestPipelineConfiguration.json").toURI())),
                    modelId
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    /**
     * Get the number of documents in a particular index
     *
     * @param indexName name of index
     * @return number of documents indexed to that index
     */
    @SneakyThrows
    protected int getDocCount(String indexName) {
        Request request = new Request("GET", "/" + indexName + "/_count");
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return (Integer) responseMap.get("count");
    }

    /**
     * Execute a search request initialized from a neural query builder
     *
     * @param index Index to search against
     * @param queryBuilder queryBuilder to produce source of query
     * @param resultSize number of results to return in the search
     * @return Search results represented as a map
     */
    protected Map<String, Object> search(String index, QueryBuilder queryBuilder, int resultSize) {
        return search(index, queryBuilder, null, resultSize);
    }

    /**
     * Execute a search request initialized from a neural query builder that can add a rescore query to the request
     *
     * @param index Index to search against
     * @param queryBuilder queryBuilder to produce source of query
     * @param  rescorer used for rescorer query builder
     * @param resultSize number of results to return in the search
     * @return Search results represented as a map
     */
    @SneakyThrows
    protected Map<String, Object> search(String index, QueryBuilder queryBuilder, QueryBuilder rescorer, int resultSize) {
        return search(index, queryBuilder, rescorer, resultSize, Map.of());
    }

    /**
     * Execute a search request initialized from a neural query builder that can add a rescore query to the request
     *
     * @param index Index to search against
     * @param queryBuilder queryBuilder to produce source of query
     * @param  rescorer used for rescorer query builder
     * @param resultSize number of results to return in the search
     * @param requestParams additional request params for search
     * @return Search results represented as a map
     */
    @SneakyThrows
    protected Map<String, Object> search(
        String index,
        QueryBuilder queryBuilder,
        QueryBuilder rescorer,
        int resultSize,
        Map<String, String> requestParams
    ) {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("query");
        queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);

        if (rescorer != null) {
            builder.startObject("rescore").startObject("query").field("query_weight", 0.0f).field("rescore_query");
            rescorer.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject().endObject();
        }

        builder.endObject();

        Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("size", Integer.toString(resultSize));
        if (requestParams != null && !requestParams.isEmpty()) {
            requestParams.forEach(request::addParameter);
        }
        request.setJsonEntity(builder.toString());

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());

        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    /**
     * Add a set of knn vectors
     *
     * @param index Name of the index
     * @param docId ID of document to be added
     * @param vectorFieldNames List of vectir fields to be added
     * @param vectors List of vectors corresponding to those fields
     */
    protected void addKnnDoc(String index, String docId, List<String> vectorFieldNames, List<Object[]> vectors) {
        addKnnDoc(index, docId, vectorFieldNames, vectors, Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Add a set of knn vectors and text to an index
     *
     * @param index Name of the index
     * @param docId ID of document to be added
     * @param vectorFieldNames List of vectir fields to be added
     * @param vectors List of vectors corresponding to those fields
     * @param textFieldNames List of text fields to be added
     * @param texts List of text corresponding to those fields
     */
    @SneakyThrows
    protected void addKnnDoc(
        String index,
        String docId,
        List<String> vectorFieldNames,
        List<Object[]> vectors,
        List<String> textFieldNames,
        List<String> texts
    ) {
        Request request = new Request("POST", "/" + index + "/_doc/" + docId + "?refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < vectorFieldNames.size(); i++) {
            builder.field(vectorFieldNames.get(i), vectors.get(i));
        }

        for (int i = 0; i < textFieldNames.size(); i++) {
            builder.field(textFieldNames.get(i), texts.get(i));
        }
        builder.endObject();

        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.CREATED, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    /**
     * Parse the first returned hit from a search response as a map
     *
     * @param searchResponseAsMap Complete search response as a map
     * @return Map of first internal hit from the search
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> getFirstInnerHit(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hits2List = (List<Object>) hits1map.get("hits");
        assertTrue(hits2List.size() > 0);
        return (Map<String, Object>) hits2List.get(0);
    }

    /**
     * Parse the total number of hits from the search
     *
     * @param searchResponseAsMap Complete search response as a map
     * @return number of hits from the search
     */
    @SuppressWarnings("unchecked")
    protected int getHitCount(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hits1List = (List<Object>) hits1map.get("hits");
        return hits1List.size();
    }

    /**
     * Create a k-NN index from a list of KNNFieldConfigs
     *
     * @param indexName of index to be created
     * @param knnFieldConfigs list of configs specifying field
     */
    @SneakyThrows
    protected void prepareKnnIndex(String indexName, List<KNNFieldConfig> knnFieldConfigs) {
        prepareKnnIndex(indexName, knnFieldConfigs, 3);
    }

    @SneakyThrows
    protected void prepareKnnIndex(String indexName, List<KNNFieldConfig> knnFieldConfigs, int numOfShards) {
        createIndexWithConfiguration(indexName, buildIndexConfiguration(knnFieldConfigs, numOfShards), "");
    }

    /**
     * Computes the expected distance between an indexVector and query text without using the neural query type.
     *
     * @param modelId ID of model to run inference
     * @param indexVector vector to compute score against
     * @param spaceType Space to measure distance
     * @param queryText Text to produce query vector from
     * @return Expected OpenSearch score for this indexVector
     */
    protected float computeExpectedScore(String modelId, float[] indexVector, SpaceType spaceType, String queryText) {
        float[] queryVector = runInference(modelId, queryText);
        return spaceType.getVectorSimilarityFunction().compare(queryVector, indexVector);
    }

    protected Map<String, Object> getTaskQueryResponse(String taskId) throws Exception {
        Response taskQueryResponse = makeRequest(
            client(),
            "GET",
            String.format(LOCALE, "_plugins/_ml/tasks/%s", taskId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(taskQueryResponse.getEntity()), false);
    }

    protected boolean checkComplete(Map<String, Object> node) {
        Predicate<Map<String, Object>> predicate = x -> node.get("error") != null || "COMPLETED".equals(String.valueOf(node.get("state")));
        return predicate.test(node);
    }

    @SneakyThrows
    private String buildIndexConfiguration(List<KNNFieldConfig> knnFieldConfigs, int numberOfShards) {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("number_of_shards", numberOfShards)
            .field("index.knn", true)
            .endObject()
            .startObject("mappings")
            .startObject("properties");

        for (KNNFieldConfig knnFieldConfig : knnFieldConfigs) {
            xContentBuilder.startObject(knnFieldConfig.getName())
                .field("type", "knn_vector")
                .field("dimension", Integer.toString(knnFieldConfig.getDimension()))
                .startObject("method")
                .field("engine", "lucene")
                .field("space_type", knnFieldConfig.getSpaceType().getValue())
                .field("name", "hnsw")
                .endObject()
                .endObject();
        }
        xContentBuilder.endObject().endObject().endObject();
        return xContentBuilder.toString();
    }

    protected static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers
    ) throws IOException {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    protected static Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        List<Header> headers,
        boolean strictDeprecationMode
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.forEach(request::addParameter);
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    protected static HttpEntity toHttpEntity(String jsonString) {
        return new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    }

    @AllArgsConstructor
    @Getter
    protected static class KNNFieldConfig {
        private final String name;
        private final Integer dimension;
        private final SpaceType spaceType;
    }

    @SneakyThrows
    protected void deleteModel(String modelId) {
        // need to undeploy first as model can be in use
        makeRequest(
            client(),
            "POST",
            String.format(LOCALE, "/_plugins/_ml/models/%s/_undeploy", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        // after model undeploy returns, the max interval to update model status is 3s in ml-commons CronJob.
        Thread.sleep(3000);

        makeRequest(
            client(),
            "DELETE",
            String.format(LOCALE, "/_plugins/_ml/models/%s", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    public boolean isUpdateClusterSettings() {
        return true;
    }

    @SneakyThrows
    protected void createSearchPipelineWithResultsPostProcessor(final String pipelineId) {
        createSearchPipeline(pipelineId, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
    }

    @SneakyThrows
    protected void createSearchPipeline(
        final String pipelineId,
        final String normalizationMethod,
        String combinationMethod,
        final Map<String, String> combinationParams
    ) {
        StringBuilder stringBuilderForContentBody = new StringBuilder();
        stringBuilderForContentBody.append("{\"description\": \"Post processor pipeline\",")
            .append("\"phase_results_processors\": [{ ")
            .append("\"normalization-processor\": {")
            .append("\"normalization\": {")
            .append("\"technique\": \"%s\"")
            .append("},")
            .append("\"combination\": {")
            .append("\"technique\": \"%s\"");
        if (Objects.nonNull(combinationParams) && !combinationParams.isEmpty()) {
            stringBuilderForContentBody.append(", \"parameters\": {");
            if (combinationParams.containsKey(PARAM_NAME_WEIGHTS)) {
                stringBuilderForContentBody.append("\"weights\": ").append(combinationParams.get(PARAM_NAME_WEIGHTS));
            }
            stringBuilderForContentBody.append(" }");
        }
        stringBuilderForContentBody.append("}").append("}}]}");
        makeRequest(
            client(),
            "PUT",
            String.format(LOCALE, "/_search/pipeline/%s", pipelineId),
            null,
            toHttpEntity(String.format(LOCALE, stringBuilderForContentBody.toString(), normalizationMethod, combinationMethod)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SneakyThrows
    protected void createSearchPipelineWithDefaultResultsPostProcessor(final String pipelineId) {
        makeRequest(
            client(),
            "PUT",
            String.format(LOCALE, "/_search/pipeline/%s", pipelineId),
            null,
            toHttpEntity(
                String.format(
                    LOCALE,
                    "{\"description\": \"Post processor pipeline\","
                        + "\"phase_results_processors\": [{ "
                        + "\"normalization-processor\": {}}]}"
                )
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    @SneakyThrows
    protected void deleteSearchPipeline(final String pipelineId) {
        makeRequest(
            client(),
            "DELETE",
            String.format(LOCALE, "/_search/pipeline/%s", pipelineId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    /**
     * Find all modesl that are currently deployed in the cluster
     * @return set of model ids
     */
    @SneakyThrows
    protected Set<String> findDeployedModels() {

        StringBuilder stringBuilderForContentBody = new StringBuilder();
        stringBuilderForContentBody.append("{")
            .append("\"query\": { \"match_all\": {} },")
            .append("  \"_source\": {")
            .append("    \"includes\": [\"model_id\"],")
            .append("    \"excludes\": [\"content\", \"model_content\"]")
            .append("}}");

        Response response = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/models/_search",
            null,
            toHttpEntity(stringBuilderForContentBody.toString()),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );

        String responseBody = EntityUtils.toString(response.getEntity());

        Map<String, Object> models = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
        Set<String> modelIds = new HashSet<>();
        if (Objects.isNull(models) || models.isEmpty()) {
            return modelIds;
        }

        Map<String, Object> hits = (Map<String, Object>) models.get("hits");
        List<Map<String, Object>> innerHitsMap = (List<Map<String, Object>>) hits.get("hits");
        return innerHitsMap.stream()
            .map(hit -> (Map<String, Object>) hit.get("_source"))
            .filter(hitsMap -> !Objects.isNull(hitsMap) && hitsMap.containsKey("model_id"))
            .map(hitsMap -> (String) hitsMap.get("model_id"))
            .collect(Collectors.toSet());
    }

    /**
     * Get the id for model currently deployed in the cluster. If there are no models deployed or it's more than 1 model
     * fail on assertion
     * @return id of deployed model
     */
    protected String getDeployedModelId() {
        Set<String> modelIds = findDeployedModels();
        assertEquals(1, modelIds.size());
        return modelIds.iterator().next();
    }

    @SneakyThrows
    private String registerModelGroup() {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        ).replace("<MODEL_GROUP_NAME>", "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8));
        Response modelGroupResponse = makeRequest(
            client(),
            "POST",
            "/_plugins/_ml/model_groups/_register",
            null,
            toHttpEntity(modelGroupRegisterRequestBody),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> modelGroupResJson = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(modelGroupResponse.getEntity()),
            false
        );
        String modelGroupId = modelGroupResJson.get("model_group_id").toString();
        assertNotNull(modelGroupId);
        return modelGroupId;
    }

    protected List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    protected Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    protected Optional<Float> getMaxScore(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return hitsMap.get("max_score") == null ? Optional.empty() : Optional.of(((Double) hitsMap.get("max_score")).floatValue());
    }

    /**
     * Enumeration for types of pipeline processors, used to lookup resources like create
     * processor request as those are type specific
     */
    protected enum ProcessorType {
        TEXT_EMBEDDING,
        TEXT_IMAGE_EMBEDDING,
        SPARSE_ENCODING
    }
}
