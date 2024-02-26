/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.visitor;

import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.neuralsearch.query.ModelInferenceQueryBuilder;

import lombok.AllArgsConstructor;

/**
 * Neural Search Query Visitor. It visits each and every component of query builder tree.
 */
@AllArgsConstructor
public class NeuralSearchQueryVisitor implements QueryBuilderVisitor {

    private final String modelId;
    private final Map<String, Object> neuralFieldMap;

    /**
     * Accept method accepts every query builder from the search request,
     * and processes it if the required conditions in accept method are satisfied.
     */
    @Override
    public void accept(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof ModelInferenceQueryBuilder) {
            ModelInferenceQueryBuilder modelInferenceQueryBuilder = (ModelInferenceQueryBuilder) queryBuilder;
            if (modelInferenceQueryBuilder.modelId() == null) {
                if (neuralFieldMap != null
                    && modelInferenceQueryBuilder.fieldName() != null
                    && neuralFieldMap.get(modelInferenceQueryBuilder.fieldName()) != null) {
                    String fieldDefaultModelId = (String) neuralFieldMap.get(modelInferenceQueryBuilder.fieldName());
                    modelInferenceQueryBuilder.modelId(fieldDefaultModelId);
                } else if (modelId != null) {
                    modelInferenceQueryBuilder.modelId(modelId);
                } else {
                    throw new IllegalArgumentException(
                        "model id must be provided in neural query or a default model id must be set in search request processor"
                    );
                }
            }
        }
    }

    /**
     * Retrieves the child visitor from the Visitor object.
     *
     * @return The sub Query Visitor
     */
    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }
}
