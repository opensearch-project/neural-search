/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import org.opensearch.neuralsearch.mapper.dto.ModelSelection;

/**
 * Interface for resolving a model_id from a {@link ModelSelection} (language option + model type). Implementations
 * resolve the model_id from operator configured cluster settings. Resolution is a cheap, local lookup and returns
 * synchronously; verification that the resolved model exists and its type matches the requested model type happens
 * later, where the model is loaded to build the semantic info field.
 */
public interface SemanticModelResolver {

    /**
     * Resolve the model_id for the given {@link ModelSelection}.
     *
     * @param modelSelection the model selection (language option + model type)
     * @return the resolved model_id
     * @throws IllegalArgumentException if no model_id is configured for the given model selection
     */
    String resolve(ModelSelection modelSelection);
}
