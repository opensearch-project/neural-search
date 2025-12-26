/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.AGENT_STEPS_FIELD_NAME;
import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.DSL_QUERY_FIELD_NAME;
import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.MEMORY_ID_FIELD_NAME;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLAgentType;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.ml.dto.AgentExecutionDTO;
import org.opensearch.neuralsearch.ml.dto.AgentInfoDTO;

import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.util.AgentQueryUtil;
import org.opensearch.neuralsearch.util.RetryUtil;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import java.util.HashMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * This class will act as an abstraction on the MLCommons client for accessing the ML Capabilities
 */
@RequiredArgsConstructor
@Log4j2
public class MLCommonsClientAccessor {
    private static final String RESPONSE_FIELD = "response";
    private static final String INDEX_NAME_FIELD = "INDEX_NAME";
    private static final String SELECTED_INDEX = "SELECTED_INDEX";
    private static final String STEPS_FIELD = "STEPS";
    private static final String QUERY_PLANNING_TOOL = "QueryPlanningTool";

    private final MachineLearningNodeClient mlClient;
    private final Cache<String, MLModel> modelCache = CacheBuilder.newBuilder().maximumSize(1000).build();

    private static final Gson gson = new Gson();

    // Error message constants for conversational agent responses
    private static final String CONVERSATIONAL_AGENT_INVALID_JSON_ERROR = "Conversational agent response does not contain valid JSON. "
        + "The agent must return a response containing a JSON object with 'dsl_query' field. "
        + "Please check the agent configuration and prompts to ensure the output is properly formatted as JSON.";

    private static final String CONVERSATIONAL_AGENT_MISSING_DSL_QUERY_ERROR =
        "No valid 'dsl_query' found in conversational agent response. "
            + "The agent must return a JSON object with 'dsl_query' field. "
            + "Please check the agent configuration and prompts.";

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
        checkModelAndThenPredict(
            inferenceRequest.getModelId(),
            listener::onFailure,
            model -> runInference(
                inferenceRequest,
                model,
                inferenceRequest.getTargetResponseFilters(),
                this::buildVectorFromResponse,
                listener
            )
        );
    }

    public void inferenceSentencesWithMapResult(
        @NonNull final TextInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        checkModelAndThenPredict(inferenceRequest.getModelId(), listener::onFailure, model -> {
            retryableInference(
                inferenceRequest,
                0,
                () -> NeuralSearchMLInputBuilder.createTextEmbeddingInput(model, null, inferenceRequest.getInputTexts(), inferenceRequest),
                this::buildMapResultFromResponse,
                listener
            );
        });
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
        checkModelAndThenPredict(inferenceRequest.getModelId(), listener::onFailure, model -> {
            retryableInference(
                inferenceRequest,
                0,
                () -> NeuralSearchMLInputBuilder.createMultimodalInputFromMap(
                    model,
                    inferenceRequest.getTargetResponseFilters(),
                    inferenceRequest.getInputObjects(),
                    inferenceRequest
                ),
                this::buildSingleVectorFromResponse,
                listener
            );
        });
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
            () -> NeuralSearchMLInputBuilder.createTextSimilarityInput(inferenceRequest.getQueryText(), inferenceRequest.getInputTexts()),
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

    private <T extends Number> List<List<T>> buildVectorFromResponse(MLOutput mlOutput) {
        final List<List<T>> vector = new ArrayList<>();
        final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();

        for (final ModelTensors tensors : tensorOutputList) {
            final List<ModelTensor> tensorsList = tensors.getMlModelTensors();
            for (final ModelTensor tensor : tensorsList) {
                // Check if we have standard tensor data first
                if (tensor.getData() != null) {
                    if (tensor.getData().length > 0) {
                        vector.add(Arrays.stream(tensor.getData()).map(value -> (T) value).collect(Collectors.toList()));
                    } else {
                        // Add empty list for empty tensor data
                        vector.add(new ArrayList<>());
                    }
                } else {
                    // Try to extract from asymmetric remote embedding model response
                    List<List<T>> remoteVectors = extractVectorsFromAsymmetricRemoteEmbeddingResponse(tensor);
                    vector.addAll(remoteVectors);
                }
            }
        }
        return vector;
    }

    /**
     * Extracts vectors from asymmetric remote embedding model response format.
     * Handles the simplified format used by asymmetric E5 remote embedding models: [[emb1], [emb2], [emb3]]
     */
    private <T extends Number> List<List<T>> extractVectorsFromAsymmetricRemoteEmbeddingResponse(ModelTensor tensor) {
        final List<List<T>> vectors = new ArrayList<>();
        Map<String, ?> dataMap = tensor.getDataAsMap();

        if (dataMap != null && !dataMap.isEmpty()) {
            Object responseData = dataMap.get(RESPONSE_FIELD);
            if (responseData instanceof List) {
                List<?> responseList = (List<?>) responseData;
                // Handle simplified format from asymmetric E5 remote embedding models: [[emb1], [emb2], [emb3]]
                for (Object embedding : responseList) {
                    if (embedding instanceof List) {
                        vectors.add(((List<?>) embedding).stream().map(v -> (T) v).collect(Collectors.toList()));
                    }
                }
            }
        }
        return vectors;
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
            if (CollectionUtils.isEmpty(tensorList)) {
                continue;
            }

            for (ModelTensor tensor : tensorList) {
                String result = tensor.getResult();
                if (result != null && !result.trim().isEmpty()) {
                    return result;
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
    public void batchInferenceSentenceHighlighting(
        @NonNull final String modelId,
        @NonNull final List<SentenceHighlightingRequest> batchRequests,
        @NonNull final FunctionName modelType,
        @NonNull final ActionListener<List<List<Map<String, Object>>>> listener
    ) {
        // Verify model type supports batch (defensive check)
        if (modelType != FunctionName.REMOTE) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Model [%s] with type [%s] does not support batch inference. "
                            + "Batch inference is only supported for REMOTE models. "
                            + "Please set 'batch_inference' to false or use a remote model.",
                        modelId,
                        modelType
                    )
                )
            );
            return;
        }

        // Create a simple InferenceRequest wrapper since batch method accepts modelId separately
        // This is different from single inference where the request object contains the modelId
        InferenceRequest inferenceRequest = new InferenceRequest() {
            @Override
            public String getModelId() {
                return modelId;
            }
        };

        retryableInference(inferenceRequest, 0, () -> {
            List<Map<String, String>> requests = batchRequests.stream()
                .map(req -> Map.of("question", req.getQuestion(), "context", req.getContext()))
                .collect(Collectors.toList());
            return NeuralSearchMLInputBuilder.createBatchHighlightingInput(requests);
        }, this::parseBatchHighlightingOutput, listener);
    }

    /**
     * Overload method for semantic highlighting with single document inference.
     * This method is used by the SemanticHighlighter for non-batch mode.
     * It defaults to QUESTION_ANSWERING model type for single document inference.
     *
     * @param inferenceRequest the request containing modelId, question, and context
     * @param listener the listener to be called with the highlighting results
     */
    public void inferenceSentenceHighlighting(
        @NonNull final SentenceHighlightingRequest inferenceRequest,
        @NonNull final ActionListener<List<Map<String, Object>>> listener
    ) {
        // For non-batch single document inference, use QUESTION_ANSWERING model type
        inferenceSentenceHighlighting(inferenceRequest, FunctionName.QUESTION_ANSWERING, listener);
    }

    /**
     * Performs sentence highlighting inference using the provided model.
     * This method will highlight relevant sentences in the context based on the question.
     * Uses the provided model type to determine the appropriate input format.
     *
     * @param inferenceRequest the request containing the question and context for highlighting
     * @param modelType the type of model (QUESTION_ANSWERING for local, REMOTE for remote)
     * @param listener the listener to be called with the highlighting results
     */
    public void inferenceSentenceHighlighting(
        @NonNull final SentenceHighlightingRequest inferenceRequest,
        @NonNull final FunctionName modelType,
        @NonNull final ActionListener<List<Map<String, Object>>> listener
    ) {
        // Use the provided model type instead of fetching it
        if (modelType == FunctionName.QUESTION_ANSWERING) {
            // Local model - use QuestionAnsweringInputDataSet
            retryableInference(
                inferenceRequest,
                0,
                () -> NeuralSearchMLInputBuilder.createQuestionAnsweringInput(
                    inferenceRequest.getQuestion(),
                    inferenceRequest.getContext()
                ),
                mlOutput -> parseSingleHighlightingOutput(mlOutput),
                listener
            );
        } else if (modelType == FunctionName.REMOTE) {
            // Remote model - use RemoteInferenceInputDataSet with inputs array
            retryableInference(
                inferenceRequest,
                0,
                () -> NeuralSearchMLInputBuilder.createSingleRemoteHighlightingInput(
                    inferenceRequest.getQuestion(),
                    inferenceRequest.getContext()
                ),
                mlOutput -> parseSingleHighlightingOutput(mlOutput),
                listener
            );
        } else {
            listener.onFailure(new IllegalArgumentException("Unsupported model type for highlighting: " + modelType));
        }
    }

    /**
     * Parse the ML output for single highlighting result
     */
    private List<Map<String, Object>> parseSingleHighlightingOutput(MLOutput mlOutput) {
        if (!(mlOutput instanceof ModelTensorOutput modelTensorOutput)) {
            throw new IllegalStateException("Expected ModelTensorOutput but got: " + mlOutput.getClass().getSimpleName());
        }

        List<ModelTensors> tensorsList = modelTensorOutput.getMlModelOutputs();
        if (tensorsList.isEmpty() || tensorsList.get(0).getMlModelTensors().isEmpty()) {
            // Return empty highlights if no results
            return List.of(Map.of(SemanticHighlightingConstants.HIGHLIGHTS_KEY, Collections.emptyList()));
        }

        Map<String, ?> dataMap = tensorsList.get(0).getMlModelTensors().get(0).getDataAsMap();
        Object highlightsObj = dataMap.get(SemanticHighlightingConstants.HIGHLIGHTS_KEY);

        // Check if the highlights are in the expected format
        if (highlightsObj == null) {
            return List.of(Map.of(SemanticHighlightingConstants.HIGHLIGHTS_KEY, Collections.emptyList()));
        }

        if (highlightsObj instanceof List<?> highlightsList && !highlightsList.isEmpty()) {
            Object firstItem = highlightsList.get(0);

            if (firstItem instanceof Map) {
                // Single document format - highlights is a list of highlight objects
                // Return the dataMap directly as it already has the correct format
                @SuppressWarnings("unchecked")
                Map<String, Object> resultMap = (Map<String, Object>) dataMap;
                return List.of(resultMap);
            } else if (firstItem instanceof List) {
                // Batch format - parse using batch method and extract first result
                List<List<Map<String, Object>>> batchResults = parseBatchHighlightingOutput(mlOutput);
                if (batchResults != null && !batchResults.isEmpty() && batchResults.get(0) != null) {
                    return List.of(Map.of(SemanticHighlightingConstants.HIGHLIGHTS_KEY, batchResults.get(0)));
                }
            }
        }

        // Default: return empty highlights
        return List.of(Map.of(SemanticHighlightingConstants.HIGHLIGHTS_KEY, Collections.emptyList()));
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
            String llmType = null;

            if (mlAgent.getMlAgent() != null && mlAgent.getMlAgent().getParameters() != null) {
                llmType = mlAgent.getMlAgent().getParameters().get("_llm_interface");
            }

            if (mlAgent.getMlAgent().getLlm() != null && mlAgent.getMlAgent().getLlm().getParameters() != null) {
                Map<String, String> parameters = mlAgent.getMlAgent().getLlm().getParameters();
                hasSystemPrompt = parameters.containsKey("system_prompt");
                hasUserPrompt = parameters.containsKey("user_prompt");
            }

            AgentInfoDTO agentInfoDTO = new AgentInfoDTO(mlAgent.getMlAgent().getType(), hasSystemPrompt, hasUserPrompt, llmType);

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
        @NonNull ActionListener<AgentExecutionDTO> listener
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
        ActionListener<AgentExecutionDTO> listener
    ) throws IOException {
        String agentType = agentInfo.getType();
        boolean hasSystemPrompt = agentInfo.isHasSystemPrompt();
        boolean hasUserPrompt = agentInfo.isHasUserPrompt();
        String llmType = agentInfo.getLlmType();

        MLAgentType type;
        try {
            type = MLAgentType.from(agentType);
        } catch (IllegalArgumentException e) {
            listener.onFailure(new IllegalArgumentException("Unsupported agent type: " + agentType));
            return;
        }

        // Validate flow agent and memory id
        if (type == MLAgentType.FLOW && agenticQuery.getMemoryId() != null) {
            throw new IllegalArgumentException("Flow agent does not support memory_id");
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put("question", agenticQuery.getQueryText());

        if (agenticQuery.getMemoryId() != null) {
            parameters.put(MEMORY_ID_FIELD_NAME, agenticQuery.getMemoryId());
        }

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

        // Add verbose as true so that the agent returns summary of agent tracing as well always
        // https://docs.opensearch.org/latest/ml-commons-plugin/api/agent-apis/execute-agent/#request-body-fields
        parameters.put("verbose", "true");

        RemoteInferenceInputDataSet dataset = RemoteInferenceInputDataSet.builder().parameters(parameters).build();
        AgentMLInput agentMLInput = new AgentMLInput(agentId, null, FunctionName.AGENT, dataset);

        if (type != MLAgentType.FLOW && type != MLAgentType.CONVERSATIONAL) {
            listener.onFailure(new IllegalArgumentException("Unsupported agent type: " + agentType));
            return;
        }

        mlClient.execute(FunctionName.AGENT, agentMLInput, ActionListener.wrap(response -> {
            MLOutput mlOutput = (MLOutput) response.getOutput();
            String dslQuery = null;
            String agentStepsSummary = null;

            String memoryId = null;
            String selectedIndex = null;
            if (type == MLAgentType.FLOW) {
                dslQuery = extractFlowAgentResult(mlOutput);
            } else if (type == MLAgentType.CONVERSATIONAL) {
                Map<String, String> conversationalResult = extractConversationalAgentResult(mlOutput, xContentRegistry, llmType);
                dslQuery = conversationalResult.get(DSL_QUERY_FIELD_NAME);
                agentStepsSummary = conversationalResult.get(AGENT_STEPS_FIELD_NAME);
                memoryId = conversationalResult.get(MEMORY_ID_FIELD_NAME);
                selectedIndex = conversationalResult.get(SELECTED_INDEX);
            }

            listener.onResponse(new AgentExecutionDTO(removeTrailingDecimalZeros(dslQuery), agentStepsSummary, memoryId, selectedIndex));
        }, e -> RetryUtil.handleRetryOrFailure(e, retryTime, () -> {
            try {
                retryableExecuteAgent(request, agenticQuery, agentId, agentInfo, xContentRegistry, retryTime + 1, listener);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }, listener)));
    }

    private String removeTrailingDecimalZeros(String query) {
        if (query == null || query.isBlank()) {
            return query;
        }

        // Remove trailing .0+ from standalone numbers (e.g., 123.00 -> 123); ignore IPs (e.g., 192.168.1.0),
        // versions (e.g., 1.0.0), and scientific notation (e.g., 1.0e5).
        final Pattern TRAILING_ZEROS_PATTERN = Pattern.compile("(?<![0-9A-Za-z.])(-?\\d+)\\.0+(?![0-9Ee.])");
        return TRAILING_ZEROS_PATTERN.matcher(query).replaceAll("$1");
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

        throw new IllegalArgumentException(
            "Flow agent did not return valid DSL query. Please check the agent configuration and ensure it returns a query."
        );
    }

    /**
     * Extract dsl_query, agent_steps_summary, and memory_id from conversational agent response
     * @param mlOutput ml output
     * @param xContentRegistry xContentRegistry
     * @param llmType llmType to determine model type
     * @return result map containing dsl_query, agent_steps_summary, and memory_id
     */
    private Map<String, String> extractConversationalAgentResult(
        MLOutput mlOutput,
        NamedXContentRegistry xContentRegistry,
        String llmType
    ) {
        boolean isClaude = llmType != null && llmType.contains(AgentQueryUtil.CLAUDE_MODEL_PREFIX);
        boolean isOpenAI = llmType != null && llmType.contains(AgentQueryUtil.OPENAI_MODEL_PREFIX);
        if (!(mlOutput instanceof ModelTensorOutput)) {
            throw new IllegalStateException("Expected ModelTensorOutput but got: " + mlOutput.getClass().getSimpleName());
        }

        ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();

        if (CollectionUtils.isEmpty(tensorOutputList)) {
            throw new IllegalStateException("Empty model result produced. Expected at least [1] tensor output, but got [0]");
        }

        Map<String, String> result = new HashMap<>();
        List<String> agentSteps = new ArrayList<>();

        // Iterate through all ModelTensors to find response and memory_id
        for (ModelTensors tensors : tensorOutputList) {
            List<ModelTensor> tensorList = tensors.getMlModelTensors();
            if (!CollectionUtils.isEmpty(tensorList)) {
                for (ModelTensor tensor : tensorList) {
                    String tensorName = tensor.getName();

                    // Extract memory_id
                    if (MEMORY_ID_FIELD_NAME.equals(tensorName)) {
                        String memoryId = tensor.getResult();
                        if (memoryId != null && !memoryId.trim().isEmpty()) {
                            result.put(MEMORY_ID_FIELD_NAME, memoryId);
                        }
                        continue;
                    }

                    // Extract response containing dsl_query or agent steps
                    if (!RESPONSE_FIELD.equals(tensorName)) {
                        continue;
                    }

                    String modelResultString = tensor.getResult();
                    if (modelResultString == null || modelResultString.isBlank()) {
                        continue;
                    }

                    // Try to extract JSON object from the response string
                    try {
                        Map<String, Object> modelResponseMap = extractJsonObjectFromString(modelResultString);

                        // Check if this response has dsl_query (final response)
                        Object dslQueryObj = modelResponseMap.get(DSL_QUERY_FIELD_NAME);
                        if (dslQueryObj != null) {
                            String dslJson = gson.toJson(dslQueryObj);
                            result.put(DSL_QUERY_FIELD_NAME, dslJson);
                        }

                        // Extract agent steps based on model type
                        if (isClaude) {
                            Map<String, String> claudeResult = extractClaudeAgentSteps(modelResponseMap);
                            if (claudeResult.containsKey(STEPS_FIELD) && !claudeResult.get(STEPS_FIELD).isEmpty()) {
                                agentSteps.add(claudeResult.get(STEPS_FIELD));
                            }
                            if (claudeResult.containsKey(INDEX_NAME_FIELD)) {
                                result.put(SELECTED_INDEX, claudeResult.get(INDEX_NAME_FIELD));
                            }
                        } else if (isOpenAI) {
                            Map<String, String> openAIResult = extractOpenAIAgentSteps(modelResponseMap);
                            if (openAIResult.containsKey(STEPS_FIELD) && !openAIResult.get(STEPS_FIELD).isEmpty()) {
                                agentSteps.add(openAIResult.get(STEPS_FIELD));
                            }
                            if (openAIResult.containsKey(INDEX_NAME_FIELD)) {
                                result.put(SELECTED_INDEX, openAIResult.get(INDEX_NAME_FIELD));
                            }
                        }
                    } catch (Exception e) {
                        // If JSON parsing fails, skip this response
                        log.debug("Skipping non-JSON response: {}", modelResultString);
                    }
                }
            }
        }

        if (!result.containsKey(DSL_QUERY_FIELD_NAME)) {
            log.error("DSL query field is missing from agent response. {}", CONVERSATIONAL_AGENT_MISSING_DSL_QUERY_ERROR);
            throw new IllegalArgumentException(CONVERSATIONAL_AGENT_MISSING_DSL_QUERY_ERROR);
        }

        // Combine agent steps if collected
        if (!agentSteps.isEmpty()) {
            result.put(AGENT_STEPS_FIELD_NAME, String.join("\n", agentSteps));
        }

        return result;
    }

    /**
    * Extracts the first JSON object from a string.
    * This is useful when the response string contains text followed by or mixed with JSON.
    *
    * @param text Input string that may contain a JSON object
    * @return Extracted JSON as Map
    * @throws IllegalArgumentException if no valid JSON object is found
    */
    private static Map<String, Object> extractJsonObjectFromString(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException(CONVERSATIONAL_AGENT_INVALID_JSON_ERROR);
        }

        ObjectMapper mapper = new ObjectMapper();

        try {
            // Find first '{' - look for JSON object only
            int startBrace = text.indexOf('{');

            if (startBrace < 0) {
                log.error("No JSON object found in text: missing opening brace. {}", CONVERSATIONAL_AGENT_INVALID_JSON_ERROR);
                throw new IllegalArgumentException(CONVERSATIONAL_AGENT_INVALID_JSON_ERROR);
            }

            // Parse JSON from the starting position - Jackson will handle finding the end
            JsonNode jsonNode = mapper.readTree(text.substring(startBrace));

            // Only return if it's a JSON object
            if (!jsonNode.isObject()) {
                log.error("Extracted JSON is not an object. {}", CONVERSATIONAL_AGENT_INVALID_JSON_ERROR);
                throw new IllegalArgumentException(CONVERSATIONAL_AGENT_INVALID_JSON_ERROR);
            }

            return mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
            });

        } catch (Exception e) {
            // Catch all JSON parsing errors and convert to IllegalArgumentException
            log.debug("Failed to extract JSON object from text", e);
            throw new IllegalArgumentException(CONVERSATIONAL_AGENT_INVALID_JSON_ERROR, e);
        }
    }

    private Map<String, String> extractClaudeAgentSteps(Map<String, Object> responseMap) {
        Map<String, String> extractedTrace = new HashMap<>();
        List<String> steps = new ArrayList<>();

        if (!responseMap.containsKey("output")) {
            return extractedTrace;
        }

        Map<String, Object> output = (Map<String, Object>) responseMap.get("output");
        if (output == null || !output.containsKey("message")) {
            return extractedTrace;
        }

        Map<String, Object> message = (Map<String, Object>) output.get("message");
        if (message == null || !message.containsKey("content")) {
            return extractedTrace;
        }

        List<Map<String, Object>> content = (List<Map<String, Object>>) message.get("content");
        if (content == null) {
            return extractedTrace;
        }

        // Check if toolUse is present in the content array
        boolean hasToolUse = content.stream().anyMatch(item -> item.containsKey("toolUse"));
        if (!hasToolUse) {
            return extractedTrace;
        }

        // Extract text and index_name from tool calls when toolUse is present
        for (Map<String, Object> item : content) {
            if (item.containsKey("text")) {
                String text = (String) item.get("text");
                if (text != null && !text.isBlank()) {
                    steps.add(text);
                }
            }
            if (item.containsKey("toolUse")) {
                Map<String, Object> toolUse = (Map<String, Object>) item.get("toolUse");
                if (toolUse != null && QUERY_PLANNING_TOOL.equals(toolUse.get("name")) && toolUse.containsKey("input")) {
                    Map<String, Object> input = (Map<String, Object>) toolUse.get("input");
                    if (input != null && input.containsKey("index_name")) {
                        String indexName = (String) input.get("index_name");
                        if (indexName != null && !indexName.isBlank()) {
                            extractedTrace.put(INDEX_NAME_FIELD, indexName);
                        }
                    }
                }
            }
        }
        extractedTrace.put(STEPS_FIELD, steps.isEmpty() ? "" : String.join("\n", steps));
        return extractedTrace;
    }

    private Map<String, String> extractOpenAIAgentSteps(Map<String, Object> responseMap) {
        Map<String, String> extractedTrace = new HashMap<>();
        List<String> steps = new ArrayList<>();

        if (!responseMap.containsKey("choices")) {
            return extractedTrace;
        }

        List<Map<String, Object>> choices = (List<Map<String, Object>>) responseMap.get("choices");
        if (choices == null || choices.isEmpty()) {
            return extractedTrace;
        }

        Map<String, Object> message = (Map<String, Object>) choices.get(0).get("message");
        if (message == null || !message.containsKey("tool_calls")) {
            return extractedTrace;
        }

        String content = (String) message.get("content");
        if (content != null && !content.isBlank()) {
            steps.add(content);
        }

        // Extract index_name from tool_calls
        List<Map<String, Object>> toolCalls = (List<Map<String, Object>>) message.get("tool_calls");
        if (toolCalls != null) {
            for (Map<String, Object> toolCall : toolCalls) {
                Map<String, Object> function = (Map<String, Object>) toolCall.get("function");
                if (function != null && QUERY_PLANNING_TOOL.equals(function.get("name")) && function.containsKey("arguments")) {
                    try {
                        Map<String, Object> argsMap = gson.fromJson((String) function.get("arguments"), Map.class);
                        String indexName = (String) argsMap.get("index_name");
                        if (indexName != null && !indexName.isBlank()) {
                            extractedTrace.put(INDEX_NAME_FIELD, indexName);
                            break;
                        }
                    } catch (Exception e) {
                        log.debug("Failed to parse tool call arguments", e);
                    }
                }
            }
        }
        extractedTrace.put(STEPS_FIELD, steps.isEmpty() ? "" : String.join("\n", steps));
        return extractedTrace;
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

    /**
     * Parse the ML output for batch highlighting results
     * Leverages buildMapResultFromResponse for consistent tensor parsing
     */
    private List<List<Map<String, Object>>> parseBatchHighlightingOutput(MLOutput mlOutput) {
        List<List<Map<String, Object>>> results = new ArrayList<>();

        // Validate ML output type
        if (mlOutput == null) {
            log.error("ML output is null in batch highlighting parsing");
            throw new IllegalStateException("ML output cannot be null");
        }

        List<Map<String, ?>> tensorMaps;
        try {
            tensorMaps = buildMapResultFromResponse(mlOutput);
        } catch (IllegalStateException e) {
            log.warn("No valid tensor output in batch highlighting response: {}", e.getMessage());
            return results;
        }

        // Process each tensor map which represents a document's highlights
        for (Map<String, ?> dataMap : tensorMaps) {
            if (dataMap == null) {
                log.warn("Null data map in tensor, adding empty result");
                results.add(new ArrayList<>());
                continue;
            }

            Object highlightsObj = dataMap.get(SemanticHighlightingConstants.HIGHLIGHTS_KEY);

            if (highlightsObj == null) {
                results.add(new ArrayList<>());
                continue;
            }

            if (!(highlightsObj instanceof List<?> highlightsList)) {
                log.error("Invalid highlights type: expected List, got: {}", highlightsObj.getClass().getSimpleName());
                throw new IllegalStateException("Expected highlights to be a List, but got: " + highlightsObj.getClass().getSimpleName());
            }

            if (highlightsList.isEmpty()) {
                results.add(new ArrayList<>());
                continue;
            }

            // Check if it's a batch response (list of lists) or single document response
            Object firstElement = highlightsList.get(0);
            if (firstElement == null) {
                results.add(new ArrayList<>());
                continue;
            }

            // Handle batch response format (list of lists)
            if (firstElement instanceof List) {
                // Process each document's highlights in the batch
                for (Object docHighlights : highlightsList) {
                    results.add(processDocumentHighlights(docHighlights));
                }
            } else if (firstElement instanceof Map) {
                // Handle single document format (list of maps)
                results.add(processDocumentHighlights(highlightsList));
            } else {
                log.error(
                    "Invalid highlights structure: expected list of lists or list of maps, got list of: {}",
                    firstElement.getClass().getSimpleName()
                );
                throw new IllegalStateException(
                    "Expected highlights to be a list of lists or list of maps, but got list of: " + firstElement.getClass().getSimpleName()
                );
            }
        }

        return results;
    }

    /**
     * Process a single document's highlights
     */
    private List<Map<String, Object>> processDocumentHighlights(Object docHighlights) {
        List<Map<String, Object>> highlights = new ArrayList<>();

        if (docHighlights == null) {
            return highlights;
        }

        if (!(docHighlights instanceof List<?> highlightList)) {
            log.error("Invalid document highlights type: expected List, got: {}", docHighlights.getClass().getSimpleName());
            throw new IllegalStateException(
                "Expected document highlights to be a List, but got: " + docHighlights.getClass().getSimpleName()
            );
        }

        for (Object item : highlightList) {
            if (item == null) {
                continue;
            }

            if (!(item instanceof Map)) {
                log.error("Invalid highlight item type: expected Map, got: {}", item.getClass().getSimpleName());
                throw new IllegalStateException("Expected highlight item to be a Map, but got: " + item.getClass().getSimpleName());
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> highlight = (Map<String, Object>) item;

            // Validate highlight structure has required fields
            if (!highlight.containsKey(SemanticHighlightingConstants.START_KEY)
                || !highlight.containsKey(SemanticHighlightingConstants.END_KEY)) {
                log.warn("Highlight missing required fields (start/end), skipping: {}", highlight);
                continue;
            }

            highlights.add(highlight);
        }

        return highlights;
    }

    /**
     * Helper method to run inference with proper MLAlgoParams handling
     */
    private <T> void runInference(
        TextInferenceRequest inferenceRequest,
        MLModel model,
        List<String> targetResponseFilters,
        Function<MLOutput, T> mlOutputBuilder,
        ActionListener<T> listener
    ) {
        retryableInference(
            inferenceRequest,
            0,
            () -> NeuralSearchMLInputBuilder.createTextEmbeddingInput(
                model,
                targetResponseFilters,
                inferenceRequest.getInputTexts(),
                inferenceRequest
            ),
            mlOutputBuilder,
            listener
        );
    }

    /**
     * Checks model and executes inference with appropriate input format.
     * Cache hit: runs immediately. Cache miss: fetches model info concurrently.
     *
     * @param modelId The model ID to check
     * @param onFailure Callback if model retrieval fails
     * @param runPrediction Callback with model object to execute inference
     */
    private void checkModelAndThenPredict(String modelId, Consumer<Exception> onFailure, Consumer<MLModel> runPrediction) {
        MLModel cached = modelCache.getIfPresent(modelId);
        if (cached != null) {
            runPrediction.accept(cached);
            return;
        }

        mlClient.getModel(modelId, null, ActionListener.<MLModel>wrap(mlModel -> {
            modelCache.put(modelId, mlModel);
            runPrediction.accept(mlModel);
        }, onFailure));
    }

    /**
     * Get cached model or fetch from ML client
     *
     * @param modelId The model ID
     * @param listener Callback with MLModel or failure
     */
    public void getCachedModel(String modelId, ActionListener<MLModel> listener) {
        MLModel cached = modelCache.getIfPresent(modelId);
        if (cached != null) {
            listener.onResponse(cached);
            return;
        }

        mlClient.getModel(modelId, null, ActionListener.<MLModel>wrap(mlModel -> {
            modelCache.put(modelId, mlModel);
            listener.onResponse(mlModel);
        }, listener::onFailure));
    }

}
