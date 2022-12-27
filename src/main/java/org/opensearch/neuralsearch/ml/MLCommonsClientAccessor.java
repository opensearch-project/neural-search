/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.ml;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.neuralsearch.util.RetryUtil;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This class will act as an abstraction on the MLCommons client for accessing the ML Capabilities
 */
@RequiredArgsConstructor
@Log4j2
public class MLCommonsClientAccessor {
    private static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    private final MachineLearningNodeClient mlClient;

    private static final int MAX_RETRY = 3;
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
        @NonNull final ActionListener<List<Float>> listener
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
        @NonNull final ActionListener<List<List<Float>>> listener
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
        @NonNull final ActionListener<List<List<Float>>> listener
    ) {
        inferenceSentencesWithRetry(targetResponseFilters, modelId, inputText, 0, listener);
    }

    private void inferenceSentencesWithRetry(
        final List<String> targetResponseFilters,
        final String modelId,
        final List<String> inputText,
        final int retryTime,
        final ActionListener<List<List<Float>>> listener
    ) {
        MLInput mlInput = createMLInput(targetResponseFilters, inputText);
        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final List<List<Float>> vector = buildVectorFromResponse(mlOutput);
            log.debug("Inference Response for input sentence {} is : {} ", inputText, vector);
            listener.onResponse(vector);
        }, e -> {
            if (RetryUtil.shouldRetry(e, retryTime)) {
                final int retryTimeAdd = retryTime + 1;
                inferenceSentencesWithRetry(targetResponseFilters, modelId, inputText, retryTimeAdd, listener);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private MLInput createMLInput(final List<String> targetResponseFilters, List<String> inputText) {
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return new MLInput(FunctionName.TEXT_EMBEDDING, null, inputDataset);
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

}
