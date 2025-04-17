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
import java.util.stream.Collectors;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.input.MLInput;
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
     * @param modelId {@link String}
     * @param inputText {@link String}
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out
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
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentences(
        @NonNull final TextInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<List<Number>>> listener
    ) {
        retryableInferenceSentencesWithVectorResult(inferenceRequest, 0, listener);
    }

    public void inferenceSentencesWithMapResult(
        @NonNull final TextInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        retryableInferenceSentencesWithMapResult(inferenceRequest, 0, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a list of floats in the order of inputText.
     *
     * @param inferenceRequest {@link InferenceRequest}
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentencesMap(@NonNull MapInferenceRequest inferenceRequest, @NonNull final ActionListener<List<Number>> listener) {
        retryableInferenceSentencesWithSingleVectorResult(inferenceRequest, 0, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient. It uses the custom model provided as modelId and the
     * {@link FunctionName#TEXT_SIMILARITY}. The return will be sent via actionListener as a list of floats representing
     * the similarity scores of the texts w.r.t. the query text, in the order of the input texts.
     *
     * @param inferenceRequest {@link InferenceRequest}
     * @param listener {@link ActionListener} receives the result of the inference
     */
    public void inferenceSimilarity(
        @NonNull SimilarityInferenceRequest inferenceRequest,
        @NonNull final ActionListener<List<Float>> listener
    ) {
        retryableInferenceSimilarityWithVectorResult(inferenceRequest, 0, listener);
    }

    private void retryableInferenceSentencesWithMapResult(
        final TextInferenceRequest inferenceRequest,
        final int retryTime,
        final ActionListener<List<Map<String, ?>>> listener
    ) {
        MLInput mlInput = createMLTextInput(null, inferenceRequest.getInputTexts());
        mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
            final List<Map<String, ?>> result = buildMapResultFromResponse(mlOutput);
            listener.onResponse(result);
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableInferenceSentencesWithMapResult(inferenceRequest, retryTime + 1, listener),
                listener
            )
        ));
    }

    private void retryableInferenceSentencesWithVectorResult(
        final TextInferenceRequest inferenceRequest,
        final int retryTime,
        final ActionListener<List<List<Number>>> listener
    ) {
        MLInput mlInput = createMLTextInput(inferenceRequest.getTargetResponseFilters(), inferenceRequest.getInputTexts());
        mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
            final List<List<Number>> vector = buildVectorFromResponse(mlOutput);
            listener.onResponse(vector);
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableInferenceSentencesWithVectorResult(inferenceRequest, retryTime + 1, listener),
                listener
            )
        ));
    }

    private void retryableInferenceSimilarityWithVectorResult(
        final SimilarityInferenceRequest inferenceRequest,
        final int retryTime,
        final ActionListener<List<Float>> listener
    ) {
        MLInput mlInput = createMLTextPairsInput(inferenceRequest.getQueryText(), inferenceRequest.getInputTexts());
        mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
            final List<Float> scores = buildVectorFromResponse(mlOutput).stream()
                .map(v -> v.getFirst().floatValue())
                .collect(Collectors.toList());
            listener.onResponse(scores);
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableInferenceSimilarityWithVectorResult(inferenceRequest, retryTime + 1, listener),
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

    private <T extends Number> List<T> buildSingleVectorFromResponse(final MLOutput mlOutput) {
        final List<List<T>> vector = buildVectorFromResponse(mlOutput);
        return vector.isEmpty() ? new ArrayList<>() : vector.get(0);
    }

    private void retryableInferenceSentencesWithSingleVectorResult(
        final MapInferenceRequest inferenceRequest,
        final int retryTime,
        final ActionListener<List<Number>> listener
    ) {
        MLInput mlInput = createMLMultimodalInput(inferenceRequest.getTargetResponseFilters(), inferenceRequest.getInputObjects());
        mlClient.predict(inferenceRequest.getModelId(), mlInput, ActionListener.wrap(mlOutput -> {
            final List<Number> vector = buildSingleVectorFromResponse(mlOutput);
            log.debug("Inference Response for input sentence is : {} ", vector);
            listener.onResponse(vector);
        },
            e -> RetryUtil.handleRetryOrFailure(
                e,
                retryTime,
                () -> retryableInferenceSentencesWithSingleVectorResult(inferenceRequest, retryTime + 1, listener),
                listener
            )
        ));
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
        retryableInferenceSentenceHighlighting(inferenceRequest, 0, listener);
    }
}
