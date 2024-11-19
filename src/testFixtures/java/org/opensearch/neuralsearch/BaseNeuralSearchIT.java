/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.opensearch.ml.common.model.MLModelState;
import static org.opensearch.neuralsearch.common.VectorUtil.vectorAsListToArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
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
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;

import static org.opensearch.neuralsearch.util.TestUtils.MAX_TASK_RESULT_QUERY_TIME_IN_SECOND;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_USER_AGENT;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.MAX_RETRY;
import static org.opensearch.neuralsearch.util.TestUtils.MAX_TIME_OUT_INTERVAL;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class BaseNeuralSearchIT extends OpenSearchSecureRestTestCase {

    protected static final Locale LOCALE = Locale.ROOT;

    protected static final Map<ProcessorType, String> PIPELINE_CONFIGS_BY_TYPE = Map.of(
        ProcessorType.TEXT_EMBEDDING,
        "processor/PipelineConfiguration.json",
        ProcessorType.SPARSE_ENCODING,
        "processor/SparseEncodingPipelineConfiguration.json",
        ProcessorType.TEXT_IMAGE_EMBEDDING,
        "processor/PipelineForTextImageEmbeddingProcessorConfiguration.json",
        ProcessorType.TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING,
        "processor/PipelineConfigurationWithNestedFieldsMapping.json",
        ProcessorType.SPARSE_ENCODING_PRUNE,
        "processor/SparseEncodingPipelineConfigurationWithPrune.json"
    );
    private static final Set<RestStatus> SUCCESS_STATUSES = Set.of(RestStatus.CREATED, RestStatus.OK);
    protected static final String CONCURRENT_SEGMENT_SEARCH_ENABLED = "search.concurrent_segment_search.enabled";

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
        updateClusterSettings("plugins.ml_commons.jvm_heap_memory_threshold", 95);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
    }

    @SneakyThrows
    protected void updateClusterSettings(final String settingKey, final Object value) {
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

    protected String registerModelGroupAndUploadModel(final String requestBody) throws Exception {
        String modelGroupId = getModelGroupId();
        // model group id is dynamically generated, we need to update model update request body after group is registered
        return uploadModel(String.format(LOCALE, requestBody, modelGroupId));
    }

    protected String uploadModel(final String requestBody) throws Exception {
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

    protected void loadModel(final String modelId) throws Exception {
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
        assertTrue(isComplete);
    }

    /**
     * Upload default model and load into the cluster
     *
     * @return modelID
     */
    @SneakyThrows
    protected String prepareModel() {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        String modelId = registerModelGroupAndUploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    /**
     * Upload default model and load into the cluster
     *
     * @return modelID
     */
    @SneakyThrows
    protected String prepareSparseEncodingModel() {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/UploadSparseEncodingModelRequestBody.json").toURI())
        );
        String modelId = registerModelGroupAndUploadModel(requestBody);
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
    protected float[] runInference(final String modelId, final String queryText) {
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

    protected void createIndexWithConfiguration(final String indexName, String indexConfiguration, final String pipelineName)
        throws Exception {
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

    protected void createPipelineProcessor(final String modelId, final String pipelineName, final ProcessorType processorType)
        throws Exception {
        createPipelineProcessor(modelId, pipelineName, processorType, null);
    }

    protected void createPipelineProcessor(
        final String modelId,
        final String pipelineName,
        final ProcessorType processorType,
        final Integer batchSize
    ) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource(PIPELINE_CONFIGS_BY_TYPE.get(processorType)).toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId, batchSize);
    }

    protected void createPipelineProcessor(
        final String requestBody,
        final String pipelineName,
        final String modelId,
        final Integer batchSize
    ) throws Exception {
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_ingest/pipeline/" + pipelineName,
            null,
            toHttpEntity(String.format(LOCALE, requestBody, modelId, batchSize == null ? 1 : batchSize)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    protected void createNeuralSparseTwoPhaseSearchProcessor(final String pipelineName) throws Exception {
        createNeuralSparseTwoPhaseSearchProcessor(pipelineName, 0.4f, 5.0f, 10000);
    }

    protected void createNeuralSparseTwoPhaseSearchProcessor(
        final String pipelineName,
        float pruneRatio,
        float expansionRate,
        int maxWindowSize
    ) throws Exception {
        String jsonTemplate = Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("processor/NeuralSparseTwoPhaseProcessorConfiguration.json")).toURI())
        );
        String customizedJson = String.format(Locale.ROOT, jsonTemplate, pruneRatio, expansionRate, maxWindowSize);
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity(customizedJson),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    protected void createSearchRequestProcessor(final String modelId, final String pipelineName) throws Exception {
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

    protected void createSearchPipelineViaConfig(String modelId, String pipelineName, String configPath) throws Exception {
        Response pipelineCreateResponse = makeRequest(
            client(),
            "PUT",
            "/_search/pipeline/" + pipelineName,
            null,
            toHttpEntity(String.format(LOCALE, Files.readString(Path.of(classLoader.getResource(configPath).toURI())), modelId)),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> node = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(pipelineCreateResponse.getEntity()),
            false
        );
        assertEquals("true", node.get("acknowledged").toString());
    }

    protected void createIndexAlias(final String index, final String alias, final QueryBuilder filterBuilder) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.startArray("actions");
        builder.startObject();
        builder.startObject("add");
        builder.field("index", index);
        builder.field("alias", alias);
        // filter object
        if (Objects.nonNull(filterBuilder)) {
            builder.field("filter");
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        builder.endObject();
        builder.endArray();
        builder.endObject();

        Request request = new Request("POST", "/_aliases");
        request.setJsonEntity(builder.toString());

        Response response = client().performRequest(request);

        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    @SneakyThrows
    protected void deleteIndexAlias(final String index, final String alias) {
        makeRequest(
            client(),
            "DELETE",
            String.format(Locale.ROOT, "%s/_alias/%s", index, alias),
            null,
            null,
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    /**
     * Get the number of documents in a particular index
     *
     * @param indexName name of index
     * @return number of documents indexed to that index
     */
    @SneakyThrows
    protected int getDocCount(final String indexName) {
        Request request = new Request("GET", "/" + indexName + "/_count");
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return (Integer) responseMap.get("count");
    }

    /**
     * Get one doc by its id
     * @param indexName index name
     * @param id doc id
     * @return map of the doc data
     */
    @SneakyThrows
    protected Map<String, Object> getDocById(final String indexName, final String id) {
        Request request = new Request("GET", "/" + indexName + "/_doc/" + id);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        return createParser(XContentType.JSON.xContent(), responseBody).map();
    }

    /**
     * Execute a search request initialized from a neural query builder
     *
     * @param index Index to search against
     * @param queryBuilder queryBuilder to produce source of query
     * @param resultSize number of results to return in the search
     * @return Search results represented as a map
     */
    protected Map<String, Object> search(final String index, final QueryBuilder queryBuilder, final int resultSize) {
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
    protected Map<String, Object> search(
        final String index,
        final QueryBuilder queryBuilder,
        final QueryBuilder rescorer,
        final int resultSize
    ) {
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
        final String index,
        final QueryBuilder queryBuilder,
        final QueryBuilder rescorer,
        final int resultSize,
        final Map<String, String> requestParams
    ) {
        return search(index, queryBuilder, rescorer, resultSize, requestParams, null);
    }

    @SneakyThrows
    protected Map<String, Object> search(
        String index,
        QueryBuilder queryBuilder,
        QueryBuilder rescorer,
        int resultSize,
        Map<String, String> requestParams,
        List<Object> aggs
    ) {
        return search(index, queryBuilder, rescorer, resultSize, requestParams, aggs, null, null, false, null, 0);
    }

    @SneakyThrows
    protected Map<String, Object> search(
        String index,
        QueryBuilder queryBuilder,
        QueryBuilder rescorer,
        int resultSize,
        Map<String, String> requestParams,
        List<Object> aggs,
        QueryBuilder postFilterBuilder,
        List<SortBuilder<?>> sortBuilders,
        boolean trackScores,
        List<Object> searchAfter,
        int from
    ) {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("from", from);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        if (rescorer != null) {
            builder.startObject("rescore").startObject("query").field("query_weight", 0.0f).field("rescore_query");
            rescorer.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject().endObject();
        }
        if (Objects.nonNull(aggs)) {
            builder.startObject("aggs");
            for (Object agg : aggs) {
                builder.value(agg);
            }
            builder.endObject();
        }
        if (Objects.nonNull(postFilterBuilder)) {
            builder.field("post_filter");
            postFilterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        if (Objects.nonNull(sortBuilders) && !sortBuilders.isEmpty()) {
            builder.startArray("sort");
            for (SortBuilder sortBuilder : sortBuilders) {
                sortBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
        }

        if (trackScores) {
            builder.field("track_scores", trackScores);
        }
        if (searchAfter != null && !searchAfter.isEmpty()) {
            builder.startArray("search_after");
            for (Object searchAfterEntry : searchAfter) {
                builder.value(searchAfterEntry);
            }
            builder.endArray();
        }

        builder.endObject();

        Request request = new Request("GET", "/" + index + "/_search?timeout=1000s");
        request.addParameter("size", Integer.toString(resultSize));
        if (requestParams != null && !requestParams.isEmpty()) {
            requestParams.forEach(request::addParameter);
        }
        logger.info("Sorting request  " + builder.toString());
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        logger.info("Response  " + responseBody);
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
    protected void addKnnDoc(final String index, final String docId, final List<String> vectorFieldNames, final List<Object[]> vectors) {
        addKnnDoc(index, docId, vectorFieldNames, vectors, Collections.emptyList(), Collections.emptyList());
    }

    @SneakyThrows
    protected void addKnnDoc(
        final String index,
        final String docId,
        final List<String> vectorFieldNames,
        final List<Object[]> vectors,
        final List<String> textFieldNames,
        final List<String> texts
    ) {
        addKnnDoc(index, docId, vectorFieldNames, vectors, textFieldNames, texts, Collections.emptyList(), Collections.emptyList());
    }

    @SneakyThrows
    protected void addKnnDoc(
        String index,
        String docId,
        List<String> vectorFieldNames,
        List<Object[]> vectors,
        List<String> textFieldNames,
        List<String> texts,
        List<String> nestedFieldNames,
        List<Map<String, String>> nestedFields
    ) {
        addKnnDoc(
            index,
            docId,
            vectorFieldNames,
            vectors,
            textFieldNames,
            texts,
            nestedFieldNames,
            nestedFields,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()
        );
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
     * @param nestedFieldNames List of nested fields to be added
     * @param nestedFields List of fields and values corresponding to those fields
     */
    @SneakyThrows
    protected void addKnnDoc(
        final String index,
        final String docId,
        final List<String> vectorFieldNames,
        final List<Object[]> vectors,
        final List<String> textFieldNames,
        final List<String> texts,
        final List<String> nestedFieldNames,
        final List<Map<String, String>> nestedFields,
        final List<String> integerFieldNames,
        final List<Integer> integerFieldValues,
        final List<String> keywordFieldNames,
        final List<String> keywordFieldValues,
        final List<String> dateFieldNames,
        final List<String> dateFieldValues
    ) {
        Request request = new Request("POST", "/" + index + "/_doc/" + docId + "?refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < vectorFieldNames.size(); i++) {
            builder.field(vectorFieldNames.get(i), vectors.get(i));
        }

        for (int i = 0; i < textFieldNames.size(); i++) {
            builder.field(textFieldNames.get(i), texts.get(i));
        }

        for (int i = 0; i < nestedFieldNames.size(); i++) {
            builder.field(nestedFieldNames.get(i));
            builder.startObject();
            Map<String, String> nestedValues = nestedFields.get(i);
            for (Map.Entry<String, String> entry : nestedValues.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        for (int i = 0; i < integerFieldNames.size(); i++) {
            builder.field(integerFieldNames.get(i), integerFieldValues.get(i));
        }

        for (int i = 0; i < keywordFieldNames.size(); i++) {
            builder.field(keywordFieldNames.get(i), keywordFieldValues.get(i));
        }

        for (int i = 0; i < dateFieldNames.size(); i++) {
            builder.field(dateFieldNames.get(i), dateFieldValues.get(i));
        }
        builder.endObject();

        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertTrue(
            request.getEndpoint() + ": failed",
            SUCCESS_STATUSES.contains(RestStatus.fromCode(response.getStatusLine().getStatusCode()))
        );
    }

    @SneakyThrows
    protected void addSparseEncodingDoc(
        final String index,
        final String docId,
        final List<String> fieldNames,
        final List<Map<String, Float>> docs
    ) {
        addSparseEncodingDoc(index, docId, fieldNames, docs, Collections.emptyList(), Collections.emptyList());
    }

    @SneakyThrows
    protected void addSparseEncodingDoc(
        final String index,
        final String docId,
        final List<String> fieldNames,
        final List<Map<String, Float>> docs,
        final List<String> textFieldNames,
        final List<String> texts
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

    protected void bulkAddDocuments(final String index, final String textField, final String pipeline, final List<Map<String, String>> docs)
        throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < docs.size(); ++i) {
            String doc = String.format(
                Locale.ROOT,
                "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%s\" } },\n" + "{ \"%s\": \"%s\"}",
                index,
                docs.get(i).get("id"),
                textField,
                docs.get(i).get("text")
            );
            builder.append(doc);
            builder.append("\n");
        }
        Request request = new Request("POST", String.format(Locale.ROOT, "/_bulk?refresh=true&pipeline=%s", pipeline));
        request.setJsonEntity(builder.toString());

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    @SneakyThrows
    protected void bulkIngest(final String ingestBulkPayload, final String pipeline) {
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
        if (Objects.nonNull(pipeline)) {
            params.put("pipeline", pipeline);
        }
        Response response = makeRequest(
            client(),
            "POST",
            "_bulk",
            params,
            toHttpEntity(ingestBulkPayload),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );

        int failedDocCount = 0;
        for (Object item : ((List) map.get("items"))) {
            Map<String, Map<String, Object>> itemMap = (Map<String, Map<String, Object>>) item;
            if (itemMap.get("index").get("error") != null) {
                failedDocCount++;
            }
        }
        assertEquals(0, failedDocCount);

        assertEquals("_bulk failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    /**
     * Parse the first returned hit from a search response as a map
     *
     * @param searchResponseAsMap Complete search response as a map
     * @return Map of first internal hit from the search
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> getFirstInnerHit(final Map<String, Object> searchResponseAsMap) {
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
    protected int getHitCount(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hits1List = (List<Object>) hits1map.get("hits");
        return hits1List.size();
    }

    /**
     * Parse the total number of hits and retrive score from the search
     *
     * @param searchResponseAsMap Complete search response as a map
     * @return number of scores list from the search
     */
    @SuppressWarnings("unchecked")
    protected List<Double> getNormalizationScoreList(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hitsList = (List<Object>) hits1map.get("hits");
        List<Double> scores = new ArrayList<>();
        for (Object hit : hitsList) {
            Map<String, Object> searchHit = (Map<String, Object>) hit;
            scores.add((Double) searchHit.get("_score"));
        }
        return scores;
    }

    /**
     * Create a k-NN index from a list of KNNFieldConfigs
     *
     * @param indexName of index to be created
     * @param knnFieldConfigs list of configs specifying field
     */
    @SneakyThrows
    protected void prepareKnnIndex(final String indexName, final List<KNNFieldConfig> knnFieldConfigs) {
        prepareKnnIndex(indexName, knnFieldConfigs, 3);
    }

    @SneakyThrows
    protected void prepareKnnIndex(final String indexName, final List<KNNFieldConfig> knnFieldConfigs, final int numOfShards) {
        createIndexWithConfiguration(indexName, buildIndexConfiguration(knnFieldConfigs, numOfShards), "");
    }

    @SneakyThrows
    protected void prepareSparseEncodingIndex(final String indexName, final List<String> sparseEncodingFieldNames) {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("mappings").startObject("properties");

        for (String fieldName : sparseEncodingFieldNames) {
            xContentBuilder.startObject(fieldName).field("type", "rank_features").endObject();
        }

        xContentBuilder.endObject().endObject().endObject();
        String indexMappings = xContentBuilder.toString();
        createIndexWithConfiguration(indexName, indexMappings, "");
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
    protected float computeExpectedScore(
        final String modelId,
        final float[] indexVector,
        final SpaceType spaceType,
        final String queryText
    ) {
        float[] queryVector = runInference(modelId, queryText);
        return spaceType.getKnnVectorSimilarityFunction().compare(queryVector, indexVector);
    }

    protected Map<String, Object> getTaskQueryResponse(final String taskId) throws Exception {
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

    protected boolean checkComplete(final Map<String, Object> node) {
        Predicate<Map<String, Object>> predicate = x -> node.get("error") != null || "COMPLETED".equals(String.valueOf(node.get("state")));
        return predicate.test(node);
    }

    @SneakyThrows
    protected String buildIndexConfiguration(final List<KNNFieldConfig> knnFieldConfigs, final int numberOfShards) {
        return buildIndexConfiguration(knnFieldConfigs, Collections.emptyList(), numberOfShards);
    }

    @SneakyThrows
    protected String buildIndexConfiguration(
        final List<KNNFieldConfig> knnFieldConfigs,
        final List<String> nestedFields,
        final int numberOfShards
    ) {
        return buildIndexConfiguration(
            knnFieldConfigs,
            nestedFields,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            numberOfShards
        );
    }

    @SneakyThrows
    protected String buildIndexConfiguration(
        final List<KNNFieldConfig> knnFieldConfigs,
        final List<String> nestedFields,
        final List<String> intFields,
        final List<String> keywordFields,
        final List<String> dateFields,
        final int numberOfShards
    ) {
        return buildIndexConfiguration(
            knnFieldConfigs,
            nestedFields,
            intFields,
            Collections.emptyList(),
            keywordFields,
            dateFields,
            numberOfShards
        );
    }

    @SneakyThrows
    protected String buildIndexConfiguration(
        final List<KNNFieldConfig> knnFieldConfigs,
        final List<String> nestedFields,
        final List<String> intFields,
        final List<String> floatFields,
        final List<String> keywordFields,
        final List<String> dateFields,
        final int numberOfShards
    ) {
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
        // treat the list in a manner that first element is always the type name and all others are keywords
        if (!nestedFields.isEmpty()) {
            String nestedFieldName = nestedFields.get(0);
            xContentBuilder.startObject(nestedFieldName).field("type", "nested");
            if (nestedFields.size() > 1) {
                xContentBuilder.startObject("properties");
                for (int i = 1; i < nestedFields.size(); i++) {
                    String innerNestedTypeField = nestedFields.get(i);
                    xContentBuilder.startObject(innerNestedTypeField).field("type", "keyword").endObject();
                }
                xContentBuilder.endObject();
            }
            xContentBuilder.endObject();
        }

        for (String intField : intFields) {
            xContentBuilder.startObject(intField).field("type", "integer").endObject();
        }

        for (String floatField : floatFields) {
            xContentBuilder.startObject(floatField).field("type", "float").endObject();
        }

        for (String keywordField : keywordFields) {
            xContentBuilder.startObject(keywordField).field("type", "keyword").endObject();
        }

        for (String dateField : dateFields) {
            xContentBuilder.startObject(dateField).field("type", "date").field("format", "MM/dd/yyyy").endObject();
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

        // wait for model undeploy to complete.
        // Sometimes the undeploy action results in a DEPLOY_FAILED state. But this does not block the model from being deleted.
        // So set both UNDEPLOYED and DEPLOY_FAILED as exit state.
        pollForModelState(modelId, Set.of(MLModelState.UNDEPLOYED, MLModelState.DEPLOY_FAILED));

        makeRequest(
            client(),
            "DELETE",
            String.format(LOCALE, "/_plugins/_ml/models/%s", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
    }

    protected void pollForModelState(String modelId, Set<MLModelState> exitModelStates) throws InterruptedException {
        MLModelState currentState = null;
        for (int i = 0; i < MAX_RETRY; i++) {
            Thread.sleep(MAX_TIME_OUT_INTERVAL);
            currentState = getModelState(modelId);
            if (exitModelStates.contains(currentState)) {
                return;
            }
        }
        fail(
            String.format(
                LOCALE,
                "Model state does not reached exit states %s after %d attempts with interval of %d ms, latest model state: %s.",
                StringUtils.join(exitModelStates, ","),
                MAX_RETRY,
                MAX_TIME_OUT_INTERVAL,
                currentState
            )
        );
    }

    @SneakyThrows
    protected MLModelState getModelState(String modelId) {
        Response getModelResponse = makeRequest(
            client(),
            "GET",
            String.format(LOCALE, "/_plugins/_ml/models/%s", modelId),
            null,
            toHttpEntity(""),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, DEFAULT_USER_AGENT))
        );
        Map<String, Object> getModelResponseJson = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(getModelResponse.getEntity()),
            false
        );
        String modelState = (String) getModelResponseJson.get("model_state");
        return MLModelState.valueOf(modelState);
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

    @SneakyThrows
    private String getModelGroupId() {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        );
        return registerModelGroup(
            String.format(LOCALE, modelGroupRegisterRequestBody, "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8))
        );
    }

    protected String registerModelGroup(final String modelGroupRegisterRequestBody) throws IOException, ParseException {
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

    // Method that waits till the health of nodes in the cluster goes green
    protected void waitForClusterHealthGreen(final String numOfNodes) throws IOException {
        Request waitForGreen = new Request("GET", "/_cluster/health");
        waitForGreen.addParameter("wait_for_nodes", numOfNodes);
        waitForGreen.addParameter("wait_for_status", "green");
        waitForGreen.addParameter("cluster_manager_timeout", "60s");
        waitForGreen.addParameter("timeout", "60s");
        client().performRequest(waitForGreen);
    }

    /**
     * Add a single Doc to an index
     *
     * @param index name of the index
     * @param docId
     * @param fieldName name of the field
     * @param text to be added
     * @param imagefieldName name of the image field
     * @param imageText name of the image text
     *
     */
    protected void addDocument(
        final String index,
        final String docId,
        final String fieldName,
        final String text,
        final String imagefieldName,
        final String imageText
    ) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + docId + "?refresh=true");

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field(fieldName, text);
        if (imagefieldName != null && imageText != null) {
            builder.field(imagefieldName, imageText);
        }
        builder.endObject();
        request.setJsonEntity(builder.toString());
        client().performRequest(request);
    }

    /**
     * Get ingest pipeline
     * @param pipelineName of the ingest pipeline
     *
     * @return get pipeline response as a map object
    */
    @SneakyThrows
    protected Map<String, Object> getIngestionPipeline(final String pipelineName) {
        Request request = new Request("GET", "/_ingest/pipeline/" + pipelineName);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return (Map<String, Object>) responseMap.get(pipelineName);
    }

    /**
     * Delete pipeline
     *
     * @param pipelineName of the pipeline
     *
     * @return delete pipeline response as a map object
     */
    @SneakyThrows
    protected Map<String, Object> deletePipeline(final String pipelineName) {
        Request request = new Request("DELETE", "/_ingest/pipeline/" + pipelineName);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        return responseMap;
    }

    protected float computeExpectedScore(final String modelId, final Map<String, Float> tokenWeightMap, final String queryText) {
        Map<String, Float> queryTokens = runSparseModelInference(modelId, queryText);
        return computeExpectedScore(tokenWeightMap, queryTokens);
    }

    protected float computeExpectedScore(final Map<String, Float> tokenWeightMap, final Map<String, Float> queryTokens) {
        Float score = 0f;
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            if (tokenWeightMap.containsKey(entry.getKey())) {
                score += entry.getValue() * getFeatureFieldCompressedNumber(tokenWeightMap.get(entry.getKey()));
            }
        }
        return score;
    }

    @SneakyThrows
    protected Map<String, Float> runSparseModelInference(final String modelId, final String queryText) {
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
    protected Float getFeatureFieldCompressedNumber(final Float originNumber) {
        int freqBits = Float.floatToIntBits(originNumber);
        freqBits = freqBits >> 15;
        freqBits = ((int) ((float) freqBits)) << 15;
        return Float.intBitsToFloat(freqBits);
    }

    // Wipe of all the resources after execution of the tests.
    protected void wipeOfTestResources(
        final String indexName,
        final String ingestPipeline,
        final String modelId,
        final String searchPipeline
    ) throws IOException {
        if (ingestPipeline != null) {
            deletePipeline(ingestPipeline);
        }
        if (searchPipeline != null) {
            deleteSearchPipeline(searchPipeline);
        }
        if (modelId != null) {
            try {
                deleteModel(modelId);
            } catch (AssertionError e) {
                // sometimes we have flaky test that the model state doesn't change after call undeploy api
                // for this case we can call undeploy api one more time
                deleteModel(modelId);
            }
        }
        if (indexName != null) {
            deleteIndex(indexName);
        }
    }

    protected Object validateDocCountAndInfo(
        String indexName,
        int expectedDocCount,
        Supplier<Map<String, Object>> documentSupplier,
        final String field,
        final Class<?> valueType
    ) {
        int count = getDocCount(indexName);
        assertEquals(expectedDocCount, count);
        Map<String, Object> document = documentSupplier.get();
        assertNotNull(document);
        Object documentSource = document.get("_source");
        assertTrue(documentSource instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> documentSourceMap = (Map<String, Object>) documentSource;
        assertTrue(documentSourceMap.containsKey(field));
        Object outputs = documentSourceMap.get(field);
        assertTrue(valueType.isAssignableFrom(outputs.getClass()));
        return outputs;
    }

    /**
     * Enumeration for types of pipeline processors, used to lookup resources like create
     * processor request as those are type specific
     */
    protected enum ProcessorType {
        TEXT_EMBEDDING,
        TEXT_EMBEDDING_WITH_NESTED_FIELDS_MAPPING,
        TEXT_IMAGE_EMBEDDING,
        SPARSE_ENCODING,
        SPARSE_ENCODING_PRUNE
    }
}
