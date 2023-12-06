/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query.visitor;

import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import lombok.AllArgsConstructor;

/**
 * Neural Search Query Visitor. It visits each and every component of query buikder tree.
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
        if (queryBuilder instanceof NeuralQueryBuilder) {
            NeuralQueryBuilder neuralQueryBuilder = (NeuralQueryBuilder) queryBuilder;
            if (neuralQueryBuilder.modelId() == null) {
                if (neuralFieldMap != null
                    && neuralQueryBuilder.fieldName() != null
                    && neuralFieldMap.get(neuralQueryBuilder.fieldName()) != null) {
                    String fieldDefaultModelId = (String) neuralFieldMap.get(neuralQueryBuilder.fieldName());
                    neuralQueryBuilder.modelId(fieldDefaultModelId);
                } else if (modelId != null) {
                    neuralQueryBuilder.modelId(modelId);
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
