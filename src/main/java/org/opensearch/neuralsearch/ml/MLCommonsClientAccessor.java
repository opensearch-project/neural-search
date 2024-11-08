/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.util.RetryUtil;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * This class will act as an abstraction on the MLCommons client for accessing the ML Capabilities
 */
@RequiredArgsConstructor
@Log4j2
public class MLCommonsClientAccessor {

    private static final int MAXIMUM_CACHE_ENTRIES = 10_000;

    /**
     * Inference parameters for calls to the MLCommons client.
     */
    @Getter
    @Builder
    public static class InferenceRequest {

        private static final List<String> DEFAULT_TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");

        private final String modelId; // required
        @Singular
        private List<String> inputTexts;
        private MLAlgoParams mlAlgoParams;
        private List<String> targetResponseFilters;
        private Map<String, String> inputObjects;
        private String queryText;

        public InferenceRequest(
            @NonNull String modelId,
            List<String> inputTexts,
            MLAlgoParams mlAlgoParams,
            List<String> targetResponseFilters,
            Map<String, String> inputObjects,
            String queryText
        ) {
            this.modelId = modelId;
            this.inputTexts = inputTexts;
            this.mlAlgoParams = mlAlgoParams;
            this.targetResponseFilters = targetResponseFilters == null ? DEFAULT_TARGET_RESPONSE_FILTERS : targetResponseFilters;
            this.inputObjects = inputObjects;
            this.queryText = queryText;
        }
    }

    private final MachineLearningNodeClient mlClient;
    private final Cache<String, Boolean> modelAsymmetryCache = CacheBuilder.<String, Boolean>builder()
        .setMaximumWeight(MAXIMUM_CACHE_ENTRIES)
        .build();

    /**
     * Wrapper around {@link #inferenceSentencesMap} that expects a single input text and produces a
     * single floating point vector as a response.
     * <p>
     * If the model is asymmetric, the {@link InferenceRequest} must contain an
     * {@link
     * org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters} s
     * {@link MLAlgoParams}. This method will check whether the model being used is asymmetric and
     * correctly handle the parameter, so it's okay to always pass the parameter (even if the model is
     * symmetric).
     *
     * @param inferenceRequest {@link InferenceRequest} containing the modelId and other parameters.
     * @param listener         {@link ActionListener} which will be called when prediction is
     *                         completed or errored out
     */
    public void inferenceSentence(@NonNull final InferenceRequest inferenceRequest, @NonNull final ActionListener<List<Float>> listener) {
        if (inferenceRequest.inputTexts.size() != 1) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Unexpected number of input texts. Expected 1 input text, but got [" + inferenceRequest.inputTexts.size() + "]"
                )
            );
            return;
        }

        inferenceSentences(inferenceRequest, ActionListener.wrap(response -> {
            if (response.size() != 1) {
                listener.onFailure(
                    new IllegalStateException(
                        "Unexpected number of vectors produced. Expected 1 vector to be returned, but got [" + response.size() + "]"
                    )
                );
                return;
            }

            listener.onResponse(response.getFirst());
        }, listener::onFailure));
    }

    /**
     * Abstraction to call predict function of api of MLClient with default targetResponse filters. It
     * uses the custom model provided as modelId and runs the {@link FunctionName#TEXT_EMBEDDING}. The
     * return will be sent using the actionListener which will have a {@link List} of {@link List} of
     * {@link Float} in the order of inputText. We are not making this function generic enough to take
     * any function or TaskType as currently we need to run only TextEmbedding tasks only.
     * <p>
     * If the model is asymmetric, the {@link InferenceRequest} must contain an
     * {@link
     * org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters} as
     * {@link MLAlgoParams}. This method will check whether the model being used is asymmetric and
     * correctly handle the parameter, so it's okay to always pass the parameter (even if the model is
     * symmetric).
     *
     * @param inferenceRequest {@link InferenceRequest} containing the modelId and other parameters.
     * @param listener         {@link ActionListener} which will be called when prediction is
     *                         completed or errored out
     */
    public void inferenceSentences(
        @NonNull final InferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<List<Float>>> listener
    ) {
        if (inferenceRequest.inputTexts.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("inputTexts must be provided"));
            return;
        }
        retryableInferenceSentencesWithVectorResult(
            inferenceRequest.targetResponseFilters,
            inferenceRequest.modelId,
            inferenceRequest.inputTexts,
            inferenceRequest.mlAlgoParams,
            0,
            listener
        );
    }

    public void inferenceSentencesWithMapResult(
        @NonNull InferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        retryableInferenceSentencesWithMapResult(
            inferenceRequest.modelId,
            inferenceRequest.inputTexts,
            inferenceRequest.mlAlgoParams,
            0,
            listener
        );
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters.
     * It uses the custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}.
     * The return will be sent using the actionListener which will have a list of floats in the order
     * of inputText.
     * <p>
     * If the model is asymmetric, the {@link InferenceRequest} must contain an
     * {@link
     * org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters} as
     * {@link MLAlgoParams}. This method will check whether the model being used is asymmetric and
     * correctly handle the parameter, so it's okay to always pass the parameter (even if the model is
     * symmetric).
     *
     * @param inferenceRequest {@link InferenceRequest} containing the modelId and other parameters.
     *                         Must contain inputObjects.
     * @param listener         {@link ActionListener} which will be called when prediction is
     *                         completed or errored out.
     */
    public void inferenceSentencesMap(
        @NonNull final InferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Float>> listener
    ) {
        if (inferenceRequest.inputObjects == null) {
            listener.onFailure(new IllegalArgumentException("inputObjects must be provided"));
            return;
        }
        retryableInferenceSentencesWithSingleVectorResult(
            inferenceRequest.targetResponseFilters,
            inferenceRequest.modelId,
            inferenceRequest.inputObjects,
            inferenceRequest.mlAlgoParams,
            0,
            listener
        );
    }

    /**
     * Abstraction to call predict function of api of MLClient. It uses the custom model provided as
     * modelId and the {@link FunctionName#TEXT_SIMILARITY}. The return will be sent via
     * actionListener as a list of floats representing the similarity scores of the texts w.r.t. the
     * query text, in the order of the input texts.
     *
     * @param inferenceRequest {@link InferenceRequest} containing the modelId and other parameters.
     *                         Must contain queryText.
     * @param listener         {@link ActionListener} receives the result of the inference
     */
    public void inferenceSimilarity(@NonNull final InferenceRequest inferenceRequest, @NonNull final ActionListener<List<Float>> listener) {
        if (inferenceRequest.queryText == null) {
            listener.onFailure(new IllegalArgumentException("queryText must be provided"));
            return;
        }
        retryableInferenceSimilarityWithVectorResult(
            inferenceRequest.modelId,
            inferenceRequest.queryText,
            inferenceRequest.inputTexts,
            0,
            listener
        );
    }

    private void retryableInferenceSentencesWithMapResult(
        final String modelId,
        final List<String> inputText,
        final MLAlgoParams mlAlgoParams,
        final int retryTime,
        final ActionListener<List<Map<String, ?>>> listener
    ) {

        Consumer<Boolean> runPrediction = isAsymmetricModel -> {
            MLInput mlInput = createMLTextInput(null, inputText, isAsymmetricModel ? mlAlgoParams : null);
            mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
                final List<Map<String, ?>> result = buildMapResultFromResponse(mlOutput);
                listener.onResponse(result);
            }, e -> {
                if (RetryUtil.shouldRetry(e, retryTime)) {
                    final int retryTimeAdd = retryTime + 1;
                    retryableInferenceSentencesWithMapResult(modelId, inputText, mlAlgoParams, retryTimeAdd, listener);
                } else {
                    listener.onFailure(e);
                }
            }));
        };

        checkModelAsymmetryAndThenPredict(modelId, listener::onFailure, runPrediction);
    }

    private void retryableInferenceSentencesWithVectorResult(
        final List<String> targetResponseFilters,
        final String modelId,
        final List<String> inputText,
        final MLAlgoParams mlAlgoParams,
        final int retryTime,
        final ActionListener<List<List<Float>>> listener
    ) {

        Consumer<Boolean> runPrediction = isAsymmetricModel -> {
            MLInput mlInput = createMLTextInput(targetResponseFilters, inputText, isAsymmetricModel ? mlAlgoParams : null);
            mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
                final List<List<Float>> vector = buildVectorFromResponse(mlOutput);
                listener.onResponse(vector);
            }, e -> {
                if (RetryUtil.shouldRetry(e, retryTime)) {
                    final int retryTimeAdd = retryTime + 1;
                    retryableInferenceSentencesWithVectorResult(
                        targetResponseFilters,
                        modelId,
                        inputText,
                        mlAlgoParams,
                        retryTimeAdd,
                        listener
                    );
                } else {
                    listener.onFailure(e);
                }
            }));
        };

        checkModelAsymmetryAndThenPredict(modelId, listener::onFailure, runPrediction);
    }

    /**
     * Check if the model is asymmetric and then run the prediction. Model asymmetry is a concept that
     * is specific to TextEmbeddingModelConfig. If the model is not a TextEmbeddingModel, then this
     * check is not applicable.
     * <p>
     * The asymmetry of a model is static for a given model. To avoid repeated checks for the same
     * model, we cache the model asymmetry status. Non-TextEmbeddingModels are cached as false.
     *
     * @param modelId       The model id to check
     * @param onFailure     The action to take if the model cannot be retrieved
     * @param runPrediction The action to take if the model is successfully retrieved
     */
    private void checkModelAsymmetryAndThenPredict(String modelId, Consumer<Exception> onFailure, Consumer<Boolean> runPrediction) {
        CheckedConsumer<MLModel, ? extends Exception> checkModelAsymmetryListener = model -> {
            MLModelConfig modelConfig = model.getModelConfig();
            if (!(modelConfig instanceof TextEmbeddingModelConfig textEmbeddingModelConfig)) {
                modelAsymmetryCache.computeIfAbsent(modelId, k -> false);
                return;
            }
            final boolean isAsymmetricModel = textEmbeddingModelConfig.getPassagePrefix() != null
                || textEmbeddingModelConfig.getQueryPrefix() != null;
            modelAsymmetryCache.computeIfAbsent(modelId, k -> isAsymmetricModel);
        };
        if (modelAsymmetryCache.get(modelId) != null) {
            runPrediction.accept(modelAsymmetryCache.get(modelId));
        } else {
            mlClient.getModel(modelId, ActionListener.<MLModel>wrap(mlModel -> {
                checkModelAsymmetryListener.accept(mlModel);
                runPrediction.accept(modelAsymmetryCache.get(modelId));
            }, onFailure));
        }
    }

    private void retryableInferenceSimilarityWithVectorResult(
        final String modelId,
        final String queryText,
        final List<String> inputText,
        final int retryTime,
        final ActionListener<List<Float>> listener
    ) {
        MLInput mlInput = createMLTextPairsInput(queryText, inputText);
        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final List<Float> scores = buildVectorFromResponse(mlOutput).stream().map(v -> v.get(0)).collect(Collectors.toList());
            listener.onResponse(scores);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                retryableInferenceSimilarityWithVectorResult(modelId, queryText, inputText, retryTime + 1, listener);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private MLInput createMLTextInput(final List<String> targetResponseFilters, List<String> inputText, MLAlgoParams mlAlgoParams) {
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return new MLInput(FunctionName.TEXT_EMBEDDING, mlAlgoParams, inputDataset);
    }

    private MLInput createMLTextPairsInput(final String query, final List<String> inputText) {
        final MLInputDataset inputDataset = new TextSimilarityInputDataSet(query, inputText);
        return new MLInput(FunctionName.TEXT_SIMILARITY, null, inputDataset);
    }

    private List<List<Float>> buildVectorFromResponse(MLOutput mlOutput) {
        final List<List<Float>> vector = new ArrayList<>();
        final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
        final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();
        for (final ModelTensors tensors : tensorOutputList) {
            final List<ModelTensor> tensorsList = tensors.getMlModelTensors();
            for (final ModelTensor tensor : tensorsList) {
                vector.add(Arrays.stream(tensor.getData()).map(value -> (Float) value).collect(Collectors.toList()));
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

    private List<Float> buildSingleVectorFromResponse(final MLOutput mlOutput) {
        final List<List<Float>> vector = buildVectorFromResponse(mlOutput);
        return vector.isEmpty() ? new ArrayList<>() : vector.get(0);
    }

    private void retryableInferenceSentencesWithSingleVectorResult(
        final List<String> targetResponseFilters,
        final String modelId,
        final Map<String, String> inputObjects,
        final MLAlgoParams mlAlgoParams,
        final int retryTime,
        final ActionListener<List<Float>> listener
    ) {

        Consumer<Boolean> predictConsumer = isAsymmetricModel -> {
            MLInput mlInput = createMLMultimodalInput(targetResponseFilters, inputObjects, isAsymmetricModel ? mlAlgoParams : null);
            mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
                final List<Float> vector = buildSingleVectorFromResponse(mlOutput);
                log.debug("Inference Response for input sentence is : {} ", vector);
                listener.onResponse(vector);
            }, e -> {
                if (RetryUtil.shouldRetry(e, retryTime)) {
                    final int retryTimeAdd = retryTime + 1;
                    retryableInferenceSentencesWithSingleVectorResult(
                        targetResponseFilters,
                        modelId,
                        inputObjects,
                        mlAlgoParams,
                        retryTimeAdd,
                        listener
                    );
                } else {
                    listener.onFailure(e);
                }
            }));
        };

        checkModelAsymmetryAndThenPredict(modelId, listener::onFailure, predictConsumer);
    }

    private MLInput createMLMultimodalInput(
        final List<String> targetResponseFilters,
        final Map<String, String> input,
        MLAlgoParams mlAlgoParams
    ) {
        List<String> inputText = new ArrayList<>();
        inputText.add(input.get(INPUT_TEXT));
        if (input.containsKey(INPUT_IMAGE)) {
            inputText.add(input.get(INPUT_IMAGE));
        }
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return new MLInput(FunctionName.TEXT_EMBEDDING, mlAlgoParams, inputDataset);
    }
}
