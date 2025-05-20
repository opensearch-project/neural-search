/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.NonNull;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;

import java.util.Locale;

import static org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer.SUPPORTED_MODEL_ALGORITHMS;
import static org.opensearch.neuralsearch.mappingtransformer.SemanticMappingTransformer.SUPPORTED_REMOTE_MODEL_TYPES;

public class SemanticMLModelUtils {
    /**
     * Get a valid model type for the semantic field from the ML model info
     * @param mlModel ML model info
     * @return A valid model type
     */
    public static String getModelType(@NonNull final MLModel mlModel) {
        final FunctionName functionName = mlModel.getAlgorithm();
        final String modelId = mlModel.getModelId();
        String modelType;
        final String errorInstruction = "After updating the model, you must update the semantic field in the index"
            + " mapping with the new model ID. If the model ID remains the same, you still need to send an update"
            + " mapping request that includes the semantic field to ensure the latest model configuration is applied.";

        switch (functionName) {
            case FunctionName.TEXT_EMBEDDING:
            case FunctionName.SPARSE_ENCODING:
            case FunctionName.SPARSE_TOKENIZE:
                modelType = functionName.name();
                break;
            case FunctionName.REMOTE:
                final MLModelConfig remoteModelConfig = mlModel.getModelConfig();
                if (remoteModelConfig == null) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Model config is required for the remote model %s used for semantic fields. %s",
                            modelId,
                            errorInstruction
                        )
                    );
                }
                final String remoteModelType = remoteModelConfig.getModelType();
                final FunctionName modelTypeFunctionName;
                final String errMsgUnsupportedRemoteModelType = String.format(
                    Locale.ROOT,
                    "Semantic field cannot support the remote model type %s with the model id %s. It should be one of [%s]. %s",
                    remoteModelType,
                    modelId,
                    String.join(",", SUPPORTED_REMOTE_MODEL_TYPES),
                    errorInstruction
                );
                try {
                    modelTypeFunctionName = FunctionName.from(remoteModelType);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(errMsgUnsupportedRemoteModelType);
                }
                modelType = switch (modelTypeFunctionName) {
                    case TEXT_EMBEDDING, SPARSE_ENCODING, SPARSE_TOKENIZE -> FunctionName.REMOTE.name()
                        + "_"
                        + modelTypeFunctionName.name();
                    default -> throw new IllegalArgumentException(errMsgUnsupportedRemoteModelType);
                };
                break;
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Semantic field cannot support the function name %s with the model id %s. It should be one of [%s]. %s",
                        functionName.name(),
                        modelId,
                        String.join(",", SUPPORTED_MODEL_ALGORITHMS),
                        errorInstruction
                    )
                );
        }
        return modelType;
    }

    /**
     * @param modelType model type
     * @return If the model is a dense model or not
     */
    public static boolean isDenseModel(@NonNull final String modelType) {
        return FunctionName.TEXT_EMBEDDING.name().equals(modelType)
            || (FunctionName.REMOTE.name() + "_" + FunctionName.TEXT_EMBEDDING.name()).equals(modelType);
    }
}
