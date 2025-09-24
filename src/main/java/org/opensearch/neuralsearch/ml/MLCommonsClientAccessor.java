/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;

import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLAgentType;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.util.RetryUtil;
import org.opensearch.ml.common.dataset.QuestionAnsweringInputDataSet;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * This class will act as an abstraction on the MLCommons client for accessing the ML Capabilities
 */
@RequiredArgsConstructor
@Log4j2
public class MLCommonsClientAccessor {
    private final MachineLearningNodeClient mlClient;
    private static final Gson gson = new Gson();

    /**
     * Wrapper around {@link #inferenceSentences} that expected a single input text and produces a single floating
     * point vector as a response.
     *
     * @param modelId   {@link String}
     * @param inputText {@link String}
     * @param listener  {@link ActionListener} which will be called when prediction is completed or errored out
     */
    public void inferenceSentence(
        @NonNull final String modelId,
        @NonNull final String inputText,
        @NonNull final ActionListener<List<Number>> listener
    ) {

        inferenceSentences(
            TextInferenceRequest.builder().modelId(modelId).inputTexts(List.of(inputText)).build(),
            ActionListener.wrap(response -> {
                if (response.size() != 1) {
                    listener.onFailure(
                        new IllegalStateException(
                            "Unexpected number of vectors produced. Expected 1 vector to be returned, but got [" + response.size() + "]"
                        )
                    );
                    return;
                }

                listener.onResponse(response.getFirst());
            }, listener::onFailure)
        );
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a {@link List} of {@link List} of {@link Float} in the order of
     * inputText. We are not making this function generic enough to take any function or TaskType as currently we
     * need to run only TextEmbedding tasks only.
     *
     * @param inferenceRequest {@link InferenceRequest}
     * @param listener         {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentences(
        @NonNull final TextInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<List<Number>>> listener
    ) {
        retryableInference(
            inferenceRequest,
            0,
            () -> createMLTextInput(inferenceRequest.getTargetResponseFilters(), inferenceRequest.getInputTexts()),
            this::buildVectorFromResponse,
            listener
        );
    }

    public void inferenceSentencesWithMapResult(
        @NonNull final TextInferenceRequest inferenceRequest,
        @Nullable MLAlgoParams mlAlgoParams,
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        retryableInference(inferenceRequest, 0, () -> {
            MLInput input = createMLTextInput(null, inferenceRequest.getInputTexts());
            if (mlAlgoParams != null) {
                input.setParameters(mlAlgoParams);
            }
            return input;
        }, this::buildMapResultFromResponse, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a list of floats in the order of inputText.
     *
     * @param inferenceRequest {@link InferenceRequest}
     * @param listener         {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentencesMap(@NonNull MapInferenceRequest inferenceRequest, @NonNull final ActionListener<List<Number>> listener) {
        retryableInference(
            inferenceRequest,
            0,
            () -> createMLMultimodalInput(inferenceRequest.getTargetResponseFilters(), inferenceRequest.getInputObjects()),
            this::buildSingleVectorFromResponse,
            listener
        );
    }

    /**
     * Abstraction to call predict function of api of MLClient. It uses the custom model provided as modelId and the
     * {@link FunctionName#TEXT_SIMILARITY}. The return will be sent via actionListener as a list of floats representing
     * the similarity scores of the texts w.r.t. the query text, in the order of the input texts.
     *
     * @param inferenceRequest {@link InferenceRequest}
     * @param listener         {@link ActionListener} receives the result of the inference
     */
    public void inferenceSimilarity(
        @NonNull SimilarityInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Float>> listener
    ) {
        retryableInference(
            inferenceRequest,
            0,
            () -> createMLTextPairsInput(inferenceRequest.getQueryText(), inferenceRequest.getInputTexts()),
            (mlOutput) -> buildVectorFromResponse(mlOutput).stream().map(v -> v.getFirst().floatValue()).collect(Collectors.toList()),
            listener
        );
    }

    /**
     * A generic function to make retryable inference request.
     * It allows caller to specify functions to vend their MLInput and process MLOutput.
     *
     * @param inferenceRequest inference request
     * @param retryTime retry time
     * @param mlInputSupplier a supplier to vend MLInput
     * @param mlOutputBuilder a consumer to consume MLOutput and provide processed output format.
     * @param listener a callback to handle result or failures.
     * @param <T> type of processed MLOutput format.
     */
    private <T> void retryableInference(
        final InferenceRequest inferenceRequest,
        final int retryTime,
        final Supplier<MLInput> mlInputSupplier,
        final Function<MLOutput, T> mlOutputBuilder,
        final ActionListener<T> listener
    ) {
        MLInput mlInput = mlInputSupplier.get();
        mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
            final T result = mlOutputBuilder.apply(mlOutput);
            listener.onResponse(result);
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableInference(inferenceRequest, retryTime + 1, mlInputSupplier, mlOutputBuilder, listener),
                listener
            )
        ));
    }

    private MLInput createMLTextInput(final List<String> targetResponseFilters, List<String> inputText) {
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return new MLInput(FunctionName.TEXT_EMBEDDING, null, inputDataset);
    }

    private MLInput createMLTextPairsInput(final String query, final List<String> inputText) {
        final MLInputDataset inputDataset = new TextSimilarityInputDataSet(query, inputText);
        return new MLInput(FunctionName.TEXT_SIMILARITY, null, inputDataset);
    }

    private <T extends Number> List<List<T>> buildVectorFromResponse(MLOutput mlOutput) {
        final List<List<T>> vector = new ArrayList<>();
        final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();
        for (final ModelTensors tensors : tensorOutputList) {
            final List<ModelTensor> tensorsList = tensors.getMlModelTensors();
            for (final ModelTensor tensor : tensorsList) {
                vector.add(Arrays.stream(tensor.getData()).map(value -> (T) value).collect(Collectors.toList()));
            }
        }
        return vector;
    }

    private List<Map<String, ?>> buildMapResultFromResponse(MLOutput mlOutput) {
        final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();
        if (CollectionUtils.isEmpty(tensorOutputList) || CollectionUtils.isEmpty(tensorOutputList.get(0).getMlModelTensors())) {
            throw new IllegalStateException(
                "Empty model result produced. Expected at least [1] tensor output and [1] model tensor, but got [0]"
            );
        }
        List<Map<String, ?>> resultMaps = new ArrayList<>();
        for (ModelTensors tensors : tensorOutputList) {
            List<ModelTensor> tensorList = tensors.getMlModelTensors();
            for (ModelTensor tensor : tensorList) {
                resultMaps.add(tensor.getDataAsMap());
            }
        }
        return resultMaps;
    }

    private String buildQueryResultFromResponseOfOutput(MLOutput mlOutput) {
        if (!(mlOutput instanceof ModelTensorOutput)) {
            throw new IllegalStateException("Expected ModelTensorOutput but got: " + mlOutput.getClass().getSimpleName());
        }
        final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;

        final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();
        if (CollectionUtils.isEmpty(tensorOutputList)) {
            throw new IllegalStateException("Empty model result produced. Expected at least [1] tensor output, but got [0]");
        }

        // Iterate through all ModelTensors to find the DSL result
        for (ModelTensors tensors : tensorOutputList) {
            List<ModelTensor> tensorList = tensors.getMlModelTensors();
            if (!CollectionUtils.isEmpty(tensorList)) {
                for (ModelTensor tensor : tensorList) {
                    String result = tensor.getResult();
                    if (result != null && !result.trim().isEmpty()) {
                        return result;
                    }
                }
            }
        }

        throw new IllegalStateException("No valid DSL result found in model output");
    }

    private <T extends Number> List<T> buildSingleVectorFromResponse(final MLOutput mlOutput) {
        final List<List<T>> vector = buildVectorFromResponse(mlOutput);
        return vector.isEmpty() ? new ArrayList<>() : vector.get(0);
    }

    /**
     * Process the highlighting output from ML model response.
     * Converts the model output into a list of maps containing highlighting information.
     */
    private List<Map<String, Object>> processHighlightingOutput(ModelTensorOutput modelTensorOutput) {
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();

            if (CollectionUtils.isEmpty(tensorOutputList)) {
                return results;
            }

            for (ModelTensors tensors : tensorOutputList) {
                List<ModelTensor> tensorsList = tensors.getMlModelTensors();

                if (CollectionUtils.isEmpty(tensorsList)) {
                    log.warn("No tensors in model output");
                    continue;
                }

                // Process each tensor in the output
                for (ModelTensor tensor : tensorsList) {
                    Map<String, ?> dataMap = tensor.getDataAsMap(); // it stored in "result" in string type
                    if (dataMap != null && !dataMap.isEmpty()) {
                        // Cast the map to Map<String, Object> - this is safe as we're only reading from it
                        @SuppressWarnings("unchecked")
                        Map<String, Object> typedDataMap = (Map<String, Object>) dataMap;
                        results.add(typedDataMap);
                    }
                }
            }

            // If no results were found, add an empty map to maintain consistent response format
            if (results.isEmpty()) {
                results.add(Collections.emptyMap());
            }

            return results;
        } catch (Exception e) {
            throw new IllegalStateException("Error processing sentence highlighting output", e);
        }
    }

    private MLInput createMLMultimodalInput(final List<String> targetResponseFilters, final Map<String, String> input) {
        List<String> inputText = new ArrayList<>();
        inputText.add(input.get(INPUT_TEXT));
        if (input.containsKey(INPUT_IMAGE)) {
            inputText.add(input.get(INPUT_IMAGE));
        }
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return new MLInput(FunctionName.TEXT_EMBEDDING, null, inputDataset);
    }

    public void getModel(@NonNull final String modelId, @NonNull final ActionListener<MLModel> listener) {
        retryableGetModel(modelId, 0, listener);
    }

    /**
     * Get model info for multiple model ids. It will send multiple getModel requests to get the model info in parallel.
     * It will fail if any one of the get model request fail. Only return the success result if all model info is
     * successfully retrieved.
     *
     * @param modelIds a set of model ids
     * @param onSuccess onSuccess consumer
     * @param onFailure onFailure consumer
     */
    public void getModels(
        @NonNull final Set<String> modelIds,
        @NonNull final Consumer<Map<String, MLModel>> onSuccess,
        @NonNull final Consumer<Exception> onFailure
    ) {
        if (modelIds.isEmpty()) {
            try {
                onSuccess.accept(Collections.emptyMap());
            } catch (Exception e) {
                onFailure.accept(e);
            }
            return;
        }

        final Map<String, MLModel> modelMap = new ConcurrentHashMap<>();
        final AtomicInteger counter = new AtomicInteger(modelIds.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);
        final List<String> errors = Collections.synchronizedList(new ArrayList<>());

        for (String modelId : modelIds) {
            try {
                getModel(modelId, ActionListener.wrap(model -> {
                    modelMap.put(modelId, model);
                    if (counter.decrementAndGet() == 0) {
                        if (hasError.get()) {
                            onFailure.accept(new RuntimeException(String.join(";", errors)));
                        } else {
                            try {
                                onSuccess.accept(modelMap);
                            } catch (Exception e) {
                                onFailure.accept(e);
                            }
                        }
                    }
                }, e -> { handleGetModelException(hasError, errors, modelId, e, counter, onFailure); }));
            } catch (Exception e) {
                handleGetModelException(hasError, errors, modelId, e, counter, onFailure);
            }
        }

    }

    private void handleGetModelException(
        AtomicBoolean hasError,
        List<String> errors,
        String modelId,
        Exception e,
        AtomicInteger counter,
        @NonNull Consumer<Exception> onFailure
    ) {
        hasError.set(true);
        errors.add("Failed to fetch model [" + modelId + "]: " + e.getMessage());
        if (counter.decrementAndGet() == 0) {
            onFailure.accept(new RuntimeException(String.join(";", errors)));
        }
    }

    private void retryableGetModel(@NonNull final String modelId, final int retryTime, @NonNull final ActionListener<MLModel> listener) {
        mlClient.getModel(
            modelId,
            null,
            ActionListener.wrap(
                listener::onResponse,
                e -> RetryUtil.handleRetryOrFailure(e, retryTime, () -> retryableGetModel(modelId, retryTime + 1, listener), listener)
            )
        );
    }

    /**
     * Retryable method to perform sentence highlighting inference.
     * This method will retry up to 3 times if a retryable exception occurs.
     */
    private void retryableInferenceSentenceHighlighting(
        final SentenceHighlightingRequest inferenceRequest,
        final int retryTime,
        final ActionListener<List<Map<String, Object>>> listener
    ) {
        try {
            MLInputDataset inputDataset = new QuestionAnsweringInputDataSet(inferenceRequest.getQuestion(), inferenceRequest.getContext());
            MLInput mlInput = new MLInput(FunctionName.QUESTION_ANSWERING, null, inputDataset);

            mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
                try {
                    List<Map<String, Object>> result = processHighlightingOutput((ModelTensorOutput) mlOutput);
                    listener.onResponse(result);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            },
                e -> RetryUtil.handleRetryOrFailure(
                    e,
                    retryTime,
                    () -> retryableInferenceSentenceHighlighting(inferenceRequest, retryTime + 1, listener),
                    listener
                )
            ));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Performs sentence highlighting inference using the provided model.
     * This method will highlight relevant sentences in the context based on the question.
     *
     * @param inferenceRequest the request containing the question and context for highlighting
     * @param listener the listener to be called with the highlighting results
     */
    public void inferenceSentenceHighlighting(
        @NonNull final SentenceHighlightingRequest inferenceRequest,
        @NonNull final ActionListener<List<Map<String, Object>>> listener
    ) {
        retryableInference(inferenceRequest, 0, () -> {
            MLInputDataset inputDataset = new QuestionAnsweringInputDataSet(inferenceRequest.getQuestion(), inferenceRequest.getContext());
            return new MLInput(FunctionName.QUESTION_ANSWERING, null, inputDataset);
        }, (mlOutput) -> processHighlightingOutput((ModelTensorOutput) mlOutput), listener);
    }

    /**
     * Get agent type from agent ID
     * @param agentId agent id
     * @param listener listener to be called with the agent type
     */
    public void getAgentDetails(@NonNull String agentId, @NonNull ActionListener<AgentInfoDTO> listener) {
        retryableGetAgentDetails(agentId, 0, listener);
    }

    /**
     * Execute get agent with retry logic
     */
    private void retryableGetAgentDetails(String agentId, int retryTime, ActionListener<AgentInfoDTO> listener) {
        mlClient.getAgent(agentId, ActionListener.wrap(mlAgent -> {
            if (mlAgent == null) {
                listener.onFailure(new IllegalStateException("Agent not found"));
                return;
            }

            boolean hasSystemPrompt = false;
            boolean hasUserPrompt = false;

            if (mlAgent.getMlAgent().getLlm() != null && mlAgent.getMlAgent().getLlm().getParameters() != null) {
                Map<String, String> parameters = mlAgent.getMlAgent().getLlm().getParameters();
                hasSystemPrompt = parameters.containsKey("system_prompt");
                hasUserPrompt = parameters.containsKey("user_prompt");
            }

            AgentInfoDTO agentInfoDTO = new AgentInfoDTO(mlAgent.getMlAgent().getType(), hasSystemPrompt, hasUserPrompt);

            listener.onResponse(agentInfoDTO);
        }, e -> RetryUtil.handleRetryOrFailure(e, retryTime, () -> retryableGetAgentDetails(agentId, retryTime + 1, listener), listener)));
    }

    /**
     * Execute agent with automatic detection of agent type
     * @param request search request
     * @param agenticQuery agentic query
     * @param agentId agent id
     * @param agentInfo agent info
     * @param xContentRegistry xContentRegistry
     * @param listener listener to be called with the agent execution result
     */
    public void executeAgent(
        @NonNull SearchRequest request,
        @NonNull AgenticSearchQueryBuilder agenticQuery,
        @NonNull String agentId,
        @NonNull AgentInfoDTO agentInfo,
        @NonNull NamedXContentRegistry xContentRegistry,
        @NonNull ActionListener<String> listener
    ) throws IOException {
        retryableExecuteAgent(request, agenticQuery, agentId, agentInfo, xContentRegistry, 0, listener);
    }

    /**
     * Execute agent with retry logic for both flow and conversational agents
     */
    private void retryableExecuteAgent(
        SearchRequest request,
        AgenticSearchQueryBuilder agenticQuery,
        String agentId,
        AgentInfoDTO agentInfo,
        NamedXContentRegistry xContentRegistry,
        int retryTime,
        ActionListener<String> listener
    ) throws IOException {
        String agentType = agentInfo.getType();
        boolean hasSystemPrompt = agentInfo.isHasSystemPrompt();
        boolean hasUserPrompt = agentInfo.isHasUserPrompt();

        MLAgentType type;
        try {
            type = MLAgentType.from(agentType);
        } catch (IllegalArgumentException e) {
            listener.onFailure(new IllegalArgumentException("Unsupported agent type: " + agentType));
            return;
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", agenticQuery.getQueryText());

        // Add index names if present
        String[] indices = request.indices();
        if (indices != null && indices.length > 0) {
            if (type == MLAgentType.FLOW && indices.length > 1) {
                throw new IllegalArgumentException("Flow agent does not support multiple indices");
            }
            parameters.put("index_name", type == MLAgentType.FLOW ? indices[0] : Arrays.toString(indices));
        }

        if (agenticQuery.getQueryFields() != null && !agenticQuery.getQueryFields().isEmpty()) {
            parameters.put("query_fields", gson.toJson(agenticQuery.getQueryFields()));
        }

        if (hasSystemPrompt == false && type != MLAgentType.FLOW) {
            parameters.put("system_prompt", loadSystemPrompt());
        }

        if (hasUserPrompt == false && type != MLAgentType.FLOW) {
            parameters.put("user_prompt", loadUserPrompt());
        }

        RemoteInferenceInputDataSet dataset = RemoteInferenceInputDataSet.builder().parameters(parameters).build();
        AgentMLInput agentMLInput = new AgentMLInput(agentId, null, FunctionName.AGENT, dataset);

        if (type != MLAgentType.FLOW && type != MLAgentType.CONVERSATIONAL) {
            listener.onFailure(new IllegalArgumentException("Unsupported agent type: " + agentType));
            return;
        }

        mlClient.execute(FunctionName.AGENT, agentMLInput, ActionListener.wrap(response -> {
            MLOutput mlOutput = (MLOutput) response.getOutput();
            String result = null;
            if (type == MLAgentType.FLOW) {
                result = extractFlowAgentResult(mlOutput);
            } else if (type == MLAgentType.CONVERSATIONAL) {
                Map<String, String> conversationalResult = extractConversationalAgentResult(mlOutput, xContentRegistry);
                result = conversationalResult.get("dsl_query");
            }

            listener.onResponse(result);
        }, e -> RetryUtil.handleRetryOrFailure(e, retryTime, () -> {
            try {
                retryableExecuteAgent(request, agenticQuery, agentId, agentInfo, xContentRegistry, retryTime + 1, listener);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }, listener)));
    }

    /**
     * Extract result from flow agent response
     * @param mlOutput ml output
     * @return result
     */
    private String extractFlowAgentResult(MLOutput mlOutput) {
        if (!(mlOutput instanceof ModelTensorOutput)) {
            throw new IllegalStateException("Expected ModelTensorOutput but got: " + mlOutput.getClass().getSimpleName());
        }
        ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();

        if (CollectionUtils.isEmpty(tensorOutputList)) {
            throw new IllegalStateException("Empty model result produced. Expected at least [1] tensor output, but got [0]");
        }

        // Iterate through all ModelTensors to find the DSL result
        for (ModelTensors tensors : tensorOutputList) {
            List<ModelTensor> tensorList = tensors.getMlModelTensors();
            if (!CollectionUtils.isEmpty(tensorList)) {
                for (ModelTensor tensor : tensorList) {
                    String result = tensor.getResult();
                    if (result != null && !result.trim().isEmpty()) {
                        return result;
                    }
                }
            }
        }

        throw new IllegalStateException("No valid DSL result found in model output");
    }

    /**
     * Extract dsl_query and agent_steps_summary from conversational agent response
     * @param mlOutput ml output
     * @param xContentRegistry xContentRegistry
     * @return result
     */
    private Map<String, String> extractConversationalAgentResult(MLOutput mlOutput, NamedXContentRegistry xContentRegistry) {
        if (!(mlOutput instanceof ModelTensorOutput)) {
            throw new IllegalStateException("Expected ModelTensorOutput but got: " + mlOutput.getClass().getSimpleName());
        }

        ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();

        if (CollectionUtils.isEmpty(tensorOutputList)) {
            throw new IllegalStateException("Empty model result produced. Expected at least [1] tensor output, but got [0]");
        }

        // Iterate through all ModelTensors to find the response
        for (ModelTensors tensors : tensorOutputList) {
            List<ModelTensor> tensorList = tensors.getMlModelTensors();
            if (!CollectionUtils.isEmpty(tensorList)) {
                for (ModelTensor tensor : tensorList) {
                    if (!"response".equals(tensor.getName())) {
                        continue;
                    }

                    Map<String, Object> dataMap = (Map<String, Object>) tensor.getDataAsMap();
                    if (dataMap == null || !dataMap.containsKey("response")) {
                        continue;
                    }

                    Object responseObj = dataMap.get("response");
                    if (!(responseObj instanceof String)) {
                        continue;
                    }

                    String responseJsonString = (String) responseObj;

                    if (responseJsonString.isBlank()) {
                        continue;
                    }

                    try (
                        XContentParser parser = XContentType.JSON.xContent()
                            .createParser(xContentRegistry, null, new BytesArray(responseJsonString).streamInput())
                    ) {

                        if (parser.currentToken() == null) {
                            parser.nextToken();
                        }

                        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                            throw new IllegalStateException("Expected START_OBJECT in response, but got: " + parser.currentToken());
                        }

                        Map<String, Object> responseMap = parser.map();
                        Object dslQueryObj = responseMap.get("dsl_query");
                        Object stepsSummaryObj = responseMap.get("agent_steps_summary");

                        Map<String, String> result = new HashMap<>();
                        if (dslQueryObj != null) {
                            try {
                                // Convert to proper JSON format using Gson
                                String dslJson = gson.toJson(dslQueryObj);
                                result.put("dsl_query", dslJson);
                            } catch (Exception e) {
                                throw new IllegalStateException("Failed to serialize dsl_query to JSON", e);
                            }
                        }
                        if (stepsSummaryObj != null) {
                            result.put("agent_steps_summary", stepsSummaryObj.toString());
                        }

                        if (!result.isEmpty()) return result;

                    } catch (IOException e) {
                        log.error("Failed to parse agent response JSON: {}", responseJsonString, e);
                        throw new IllegalStateException("Failed to parse agent response using XContentParser: " + e.getMessage(), e);
                    }
                }
            }
        }

        throw new IllegalStateException("No valid 'dsl_query' found in conversational agent response");
    }

    /**
     * Load system prompt from resources file
     */
    private String loadSystemPrompt() throws IOException {
        var inputStream = getClass().getClassLoader().getResourceAsStream("agentic-system-prompt.txt");
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Load user prompt from resources file
     */
    private String loadUserPrompt() throws IOException {
        var inputStream = getClass().getClassLoader().getResourceAsStream("agentic-user-prompt.txt");
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

}
