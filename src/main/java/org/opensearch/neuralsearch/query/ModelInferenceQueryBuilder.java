/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

/**
 * Query builders which calls ml-commons API to do model inference.
 * The model inference result is used for search on target field.
 */
public interface ModelInferenceQueryBuilder {
    /**
     * Get the model id used by ml-commons model inference. Return null if the model id is absent.
     */
    public String modelId();

    /**
     * Set a new model id for the query builder.
     */
    public ModelInferenceQueryBuilder modelId(String modelId);

    /**
     * Get the field name for search.
     */
    public String fieldName();
}
