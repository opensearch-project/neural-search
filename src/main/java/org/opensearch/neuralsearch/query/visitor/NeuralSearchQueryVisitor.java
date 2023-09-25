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

public class NeuralSearchQueryVisitor implements QueryBuilderVisitor {

    private String modelId;
    private Map<String, Object> neuralFieldMap;

    public NeuralSearchQueryVisitor(String modelId, Map<String, Object> neuralFieldMap) {
        this.modelId = modelId;
        this.neuralFieldMap = neuralFieldMap;
    }

    @Override
    public void accept(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof NeuralQueryBuilder) {
            NeuralQueryBuilder neuralQueryBuilder = (NeuralQueryBuilder) queryBuilder;
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

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }
}