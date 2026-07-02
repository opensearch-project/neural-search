/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Locale;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.TextSimilarityInputDataSet;
import org.opensearch.ml.common.dataset.QuestionAnsweringInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.neuralsearch.processor.InferenceRequest;
import org.opensearch.neuralsearch.processor.EmbeddingContentType;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;

/**
 * Builder class for creating MLInput objects for all model types (local/remote, symmetric/asymmetric).
 * Handles the different input formats required by each model type.
 * Renamed from MLInputBuilder to avoid conflicts with ml-commons.
 */
public class NeuralSearchMLInputBuilder {

    /**
     * Creates MLInput for text embedding inference, supporting all model types.
     * Automatically detects model type (local/remote) and asymmetric behavior.
     *
     * @param model The ML model
     * @param targetResponseFilters Response filters (used for local models only)
     * @param inputText Input text list
     * @param inferenceRequest Inference request with optional content type for asymmetric models
     * @return MLInput configured for the model type
     */
    public static MLInput createTextEmbeddingInput(
        MLModel model,
        List<String> targetResponseFilters,
        List<String> inputText,
        InferenceRequest inferenceRequest
    ) {
        boolean isAsymmetric = AsymmetricModelDetector.isAsymmetricModel(model);
        MLModelConfig modelConfig = model.getModelConfig();

        if (isAsymmetric && modelConfig instanceof RemoteModelConfig) {
            return createAsymmetricRemoteInput(inputText, inferenceRequest);
        }

        MLAlgoParams mlAlgoParams = createMLAlgoParams(isAsymmetric, inferenceRequest);
        ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return createMLInput(inputDataset, mlAlgoParams);
    }

    private static MLInput createRemoteInput(Map<String, String> parameters) {
        return createMLInput(new RemoteInferenceInputDataSet(parameters), null);
    }

    private static MLInput createMLInput(MLInputDataset inputDataset, MLAlgoParams mlAlgoParams) {
        // FunctionName is not used by ml-commons for inference, so any value can be passed.
        return new MLInput(FunctionName.REMOTE, mlAlgoParams, inputDataset);
    }

    /**
     * Creates MLAlgoParams for local model inference.
     * For asymmetric models, adds content type parameter.
     */
    private static MLAlgoParams createMLAlgoParams(boolean isAsymmetric, InferenceRequest inferenceRequest) {
        if (!isAsymmetric) {
            return inferenceRequest.getMlAlgoParams();
        }

        EmbeddingContentType contentType = inferenceRequest.getEmbeddingContentType();
        if (contentType == null) {
            throw new IllegalArgumentException("embeddingContentType must be set for asymmetric local models");
        }

        MLAlgoParams presetParams = inferenceRequest.getMlAlgoParams();
        if (presetParams != null && !(presetParams instanceof AsymmetricTextEmbeddingParameters)) {
            throw new IllegalArgumentException("MLAlgoParams must be AsymmetricTextEmbeddingParameters for asymmetric models");
        }

        return (presetParams != null
            ? ((AsymmetricTextEmbeddingParameters) presetParams).toBuilder()
            : AsymmetricTextEmbeddingParameters.builder()).embeddingContentType(contentType.toMLContentType()).build();
    }

    /**
     * Creates MLInput for text similarity inference.
     */
    public static MLInput createTextSimilarityInput(String query, List<String> inputText) {
        MLInputDataset inputDataset = new TextSimilarityInputDataSet(query, inputText);
        return createMLInput(inputDataset, null);
    }

    /**
     * Creates MLInput for question answering (local highlighting).
     */
    public static MLInput createQuestionAnsweringInput(String question, String context) {
        MLInputDataset inputDataset = new QuestionAnsweringInputDataSet(question, context);
        return createMLInput(inputDataset, null);
    }

    /**
     * Creates MLInput for remote inference with parameters.
     */
    public static MLInput createRemoteHighlightingInput(Map<String, String> parameters) {
        return createRemoteInput(new HashMap<>(parameters));
    }

    /**
     * Creates MLInput for batch highlighting with multiple question-context pairs.
     */
    public static MLInput createBatchHighlightingInput(List<Map<String, String>> batchRequests) {
        try {
            Map<String, String> parameters = new HashMap<>();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startArray();
            for (Map<String, String> request : batchRequests) {
                builder.startObject()
                    .field(SemanticHighlightingConstants.QUESTION_KEY, request.get("question"))
                    .field(SemanticHighlightingConstants.CONTEXT_KEY, request.get("context"))
                    .endObject();
            }
            builder.endArray();
            parameters.put(SemanticHighlightingConstants.INPUTS_KEY, builder.toString());
            return createRemoteInput(parameters);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create batch highlighting ML input", e);
        }
    }

    /**
     * Creates MLInput for single remote highlighting request.
     */
    public static MLInput createSingleRemoteHighlightingInput(String question, String context) {
        try {
            Map<String, String> parameters = new HashMap<>();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startArray();
            builder.startObject()
                .field(SemanticHighlightingConstants.QUESTION_KEY, question)
                .field(SemanticHighlightingConstants.CONTEXT_KEY, context)
                .endObject();
            builder.endArray();
            parameters.put(SemanticHighlightingConstants.INPUTS_KEY, builder.toString());
            return createRemoteInput(parameters);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create remote highlighting ML input", e);
        }
    }

    /**
     * Creates MLInput for asymmetric remote models.
     * Uses XContentBuilder to ensure proper JSON serialization for texts array.
     */
    private static MLInput createAsymmetricRemoteInput(List<String> inputText, InferenceRequest inferenceRequest) {
        try {
            Map<String, String> parameters = new HashMap<>();

            XContentBuilder textsBuilder = XContentFactory.jsonBuilder();
            textsBuilder.startArray();
            for (String text : inputText) {
                textsBuilder.value(text);
            }
            textsBuilder.endArray();
            parameters.put(AsymmetricTextEmbeddingConstants.TEXTS_KEY, textsBuilder.toString());

            EmbeddingContentType contentType = inferenceRequest.getEmbeddingContentType();
            parameters.put(AsymmetricTextEmbeddingConstants.CONTENT_TYPE_KEY, contentType.toString().toLowerCase(Locale.ROOT));

            return createRemoteInput(parameters);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create asymmetric remote ML input", e);
        }
    }

    /**
     * Creates MLInput for multimodal text+image embedding from input map.
     * Uses original createMLMultimodalInput logic with asymmetric model support.
     */
    public static MLInput createMultimodalInputFromMap(
        MLModel model,
        List<String> targetResponseFilters,
        Map<String, String> inputObjects,
        InferenceRequest inferenceRequest
    ) {
        List<String> inputText = new ArrayList<>();
        inputText.add(inputObjects.get(INPUT_TEXT));
        if (inputObjects.containsKey(INPUT_IMAGE)) {
            inputText.add(inputObjects.get(INPUT_IMAGE));
        }

        // Handle asymmetric models
        boolean isAsymmetric = AsymmetricModelDetector.isAsymmetricModel(model);
        MLModelConfig modelConfig = model.getModelConfig();

        if (isAsymmetric && modelConfig instanceof RemoteModelConfig) {
            return createAsymmetricRemoteInput(inputText, inferenceRequest);
        }

        MLAlgoParams mlAlgoParams = null;
        if (isAsymmetric) {
            EmbeddingContentType contentType = inferenceRequest.getEmbeddingContentType();
            if (contentType != null) {
                mlAlgoParams = AsymmetricTextEmbeddingParameters.builder().embeddingContentType(contentType.toMLContentType()).build();
            }
        }

        ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        return createMLInput(inputDataset, mlAlgoParams);
    }
}
