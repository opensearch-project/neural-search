/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.util.Map;

import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;

/**
 * Utility class for detecting asymmetric models in both local and remote configurations.
 * Asymmetric models treat queries and passages differently, typically using different prefixes.
 */
public class AsymmetricModelDetector {

    /**
     * Determines if a model is asymmetric by checking for query/passage prefixes.
     * Supports both local models (TextEmbeddingModelConfig) and remote models (RemoteModelConfig).
     *
     * @param model The ML model to check
     * @return true if model has asymmetric characteristics, false otherwise
     */
    public static boolean isAsymmetricModel(MLModel model) {
        if (model == null) {
            return false;
        }

        MLModelConfig modelConfig = model.getModelConfig();
        if (modelConfig == null) {
            return false;
        }

        // Check local models
        if (modelConfig instanceof TextEmbeddingModelConfig textEmbeddingModelConfig) {
            return isAsymmetricLocalModel(textEmbeddingModelConfig);
        }

        // Check remote models
        if (modelConfig instanceof RemoteModelConfig remoteModelConfig) {
            return isAsymmetricRemoteModel(remoteModelConfig);
        }

        return false;
    }

    /**
     * Checks if a local model (TextEmbeddingModelConfig) is asymmetric.
     */
    private static boolean isAsymmetricLocalModel(TextEmbeddingModelConfig config) {
        return config.getPassagePrefix() != null || config.getQueryPrefix() != null;
    }

    /**
     * Checks if a remote model is asymmetric by looking for asymmetric identifiers
     * in the additionalConfig or by checking known asymmetric model patterns.
     */
    private static boolean isAsymmetricRemoteModel(RemoteModelConfig remoteConfig) {
        Map<String, Object> additionalConfig = remoteConfig.getAdditionalConfig();

        if (additionalConfig != null) {
            // Method 1: Check for explicit asymmetric flag
            Object isAsymmetric = additionalConfig.get("is_asymmetric");
            if (isAsymmetric instanceof Boolean && (Boolean) isAsymmetric) {
                return true;
            }

            // Method 2: Check for query/passage prefix indicators
            if (additionalConfig.containsKey(AsymmetricTextEmbeddingConstants.QUERY_PREFIX_CONFIG_KEY)
                || additionalConfig.containsKey(AsymmetricTextEmbeddingConstants.PASSAGE_PREFIX_CONFIG_KEY)) {
                return true;
            }
        }

        return false;
    }
}
