/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.execute.agent.AgentMLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
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
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        retryableInference(
            inferenceRequest,
            0,
            () -> createMLTextInput(null, inferenceRequest.getInputTexts()),
            this::buildMapResultFromResponse,
            listener
        );
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
     * Execute agent with provided parameters and return DSL query string.
     *
     * @param agentId    the agent ID to execute
     * @param parameters the parameters to pass to the agent
     * @param listener   the listener to be called with the DSL query result
     */
    public void executeAgent(
        @NonNull final String agentId,
        @NonNull final Map<String, String> parameters,
        @NonNull final ActionListener<String> listener
    ) {
        retryableExecuteAgent(agentId, parameters, 0, listener);
    }

    private void retryableExecuteAgent(
        final String agentId,
        final Map<String, String> parameters,
        final int retryTime,
        final ActionListener<String> listener
    ) {
        RemoteInferenceInputDataSet dataset = RemoteInferenceInputDataSet.builder().parameters(parameters).build();
        AgentMLInput agentMLInput = new AgentMLInput(agentId, null, FunctionName.AGENT, dataset);
        mlClient.execute(FunctionName.AGENT, agentMLInput, ActionListener.wrap(response -> {
            try {
                // Extract DSL query from inference results following the structure:
                MLOutput mlOutput = (MLOutput) response.getOutput();
                final String inferenceResults = buildQueryResultFromResponseOfOutput(mlOutput);

                listener.onResponse(inferenceResults);
            } catch (Exception e) {
                listener.onFailure(new IllegalStateException("Failed to extract result from agent response", e));
            }
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableExecuteAgent(agentId, parameters, retryTime + 1, listener),
                listener
            )
        ));
    }
}
