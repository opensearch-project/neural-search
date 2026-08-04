/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import org.opensearch.core.action.ActionListener;

/**
 * Interface for resolving a pretrained model based on language option and model type.
 * Implementations look up or register+deploy a model and return the model_id asynchronously.
 */
public interface SemanticModelResolver {

    /**
     * Resolve the model_id for the given language option and model type combination.
     * The resolved model_id is returned via the listener.
     *
     * @param languageOption the language option (e.g., "ENGLISH", "MULTILINGUAL")
     * @param modelType the model type (e.g., "SPARSE", "DENSE")
     * @param listener action listener that receives the resolved model_id
     */
    void resolve(String languageOption, String modelType, ActionListener<String> listener);

    /**
     * Validate that the language option and model type values are recognized.
     * Throws IllegalArgumentException if the values are not supported.
     *
     * @param languageOption the language option to validate
     * @param modelType the model type to validate
     */
    default void validate(String languageOption, String modelType) {
        // Default implementation does nothing; subclasses override
    }
}
