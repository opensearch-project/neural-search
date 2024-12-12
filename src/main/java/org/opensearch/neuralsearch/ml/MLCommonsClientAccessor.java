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
import java.util.stream.Collectors;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.input.MLInput;
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
    private static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    private final MachineLearningNodeClient mlClient;

    /**
     * Wrapper around {@link #inferenceSentences} that expected a single input text and produces a single floating
     * point vector as a response.
     *
     * @param modelId {@link String}
     * @param inputText {@link List} of {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out
     */
    public void inferenceSentence(
        @NonNull final String modelId,
        @NonNull final String inputText,
        @NonNull final ActionListener<List<Number>> listener
    ) {
        inferenceSentences(TARGET_RESPONSE_FILTERS, modelId, List.of(inputText), ActionListener.wrap(response -> {
            if (response.size() != 1) {
                listener.onFailure(
                    new IllegalStateException(
                        "Unexpected number of vectors produced. Expected 1 vector to be returned, but got [" + response.size() + "]"
                    )
                );
                return;
            }

            listener.onResponse(response.get(0));
        }, listener::onFailure));
    }

    /**
     * Abstraction to call predict function of api of MLClient with default targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a {@link List} of {@link List} of {@link Float} in the order of
     * inputText. We are not making this function generic enough to take any function or TaskType as currently we
     * need to run only TextEmbedding tasks only.
     *
     * @param modelId {@link String}
     * @param inputText {@link List} of {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out
     */
    public void inferenceSentences(
        @NonNull final String modelId,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<List<Number>>> listener
    ) {
        inferenceSentences(TARGET_RESPONSE_FILTERS, modelId, inputText, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a {@link List} of {@link List} of {@link Float} in the order of
     * inputText. We are not making this function generic enough to take any function or TaskType as currently we
     * need to run only TextEmbedding tasks only.
     *
     * @param targetResponseFilters {@link List} of {@link String} which filters out the responses
     * @param modelId {@link String}
     * @param inputText {@link List} of {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentences(
        @NonNull final List<String> targetResponseFilters,
        @NonNull final String modelId,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<List<Number>>> listener
    ) {
        retryableInferenceSentencesWithVectorResult(targetResponseFilters, modelId, inputText, 0, listener);
    }

    public void inferenceSentencesWithMapResult(
        @NonNull final String modelId,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<Map<String, ?>>> listener
    ) {
        retryableInferenceSentencesWithMapResult(modelId, inputText, 0, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link FunctionName#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a list of floats in the order of inputText.
     *
     * @param modelId {@link String}
     * @param inputObjects {@link Map} of {@link String}, {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentences(
        @NonNull final String modelId,
        @NonNull final Map<String, String> inputObjects,
        @NonNull final ActionListener<List<Number>> listener
    ) {
        retryableInferenceSentencesWithSingleVectorResult(TARGET_RESPONSE_FILTERS, modelId, inputObjects, 0, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient. It uses the custom model provided as modelId and the
     * {@link FunctionName#TEXT_SIMILARITY}. The return will be sent via actionListener as a list of floats representing
     * the similarity scores of the texts w.r.t. the query text, in the order of the input texts.
     *
     * @param modelId {@link String} ML-Commons Model Id
     * @param queryText {@link String} The query to compare all the inputText to
     * @param inputText {@link List} of {@link String} The texts to compare to the query
     * @param listener {@link ActionListener} receives the result of the inference
     */
    public void inferenceSimilarity(
        @NonNull final String modelId,
        @NonNull final String queryText,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<Float>> listener
    ) {
        retryableInferenceSimilarityWithVectorResult(modelId, queryText, inputText, 0, listener);
    }

    private void retryableInferenceSentencesWithMapResult(
        final String modelId,
        final List<String> inputText,
        final int retryTime,
        final ActionListener<List<Map<String, ?>>> listener
    ) {
        MLInput mlInput = createMLTextInput(null, inputText);
        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final List<Map<String, ?>> result = buildMapResultFromResponse(mlOutput);
            listener.onResponse(result);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                final int retryTimeAdd = retryTime + 1;
                retryableInferenceSentencesWithMapResult(modelId, inputText, retryTimeAdd, listener);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void retryableInferenceSentencesWithVectorResult(
        final List<String> targetResponseFilters,
        final String modelId,
        final List<String> inputText,
        final int retryTime,
        final ActionListener<List<List<Number>>> listener
    ) {
        MLInput mlInput = createMLTextInput(targetResponseFilters, inputText);
        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final List<List<Number>> vector = buildVectorFromResponse(mlOutput);
            listener.onResponse(vector);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                final int retryTimeAdd = retryTime + 1;
                retryableInferenceSentencesWithVectorResult(targetResponseFilters, modelId, inputText, retryTimeAdd, listener);
            } else {
                listener.onFailure(e);
            }
        }));
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
            final List<List<Float>> tensors = buildVectorFromResponse(mlOutput);
            final List<Float> scores = tensors.stream().map(v -> v.get(0)).collect(Collectors.toList());
            listener.onResponse(scores);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                retryableInferenceSimilarityWithVectorResult(modelId, queryText, inputText, retryTime + 1, listener);
            } else {
                listener.onFailure(e);
            }
        }));
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
        final List<String> targetResponseFilters,
        final String modelId,
        final Map<String, String> inputObjects,
        final int retryTime,
        final ActionListener<List<Number>> listener
    ) {
        MLInput mlInput = createMLMultimodalInput(targetResponseFilters, inputObjects);
        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final List<Number> vector = buildSingleVectorFromResponse(mlOutput);
            log.debug("Inference Response for input sentence is : {} ", vector);
            listener.onResponse(vector);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                final int retryTimeAdd = retryTime + 1;
                retryableInferenceSentencesWithSingleVectorResult(targetResponseFilters, modelId, inputObjects, retryTimeAdd, listener);
            } else {
                listener.onFailure(e);
            }
        }));
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
}
