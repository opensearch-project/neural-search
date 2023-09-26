/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.visitor.NeuralSearchQueryVisitor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

public class NeuralQueryProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "neural_query";

    final String modelId;

    final Map<String, Object> neuralFieldMap;

    /**
     * Returns the type of the processor.
     *
     * @return The processor type.
     */
    @Override
    public String getType() {
        return TYPE;
    }

    protected NeuralQueryProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String modelId,
        Map<String, Object> fieldInfoMap
    ) {
        super(tag, description, ignoreFailure);
        this.modelId = modelId;
        this.neuralFieldMap = fieldInfoMap;
    }

    @Override
    public SearchRequest processRequest(SearchRequest searchRequest) throws Exception {
        QueryBuilder queryBuilder = searchRequest.source().query();
        queryBuilder.visit(new NeuralSearchQueryVisitor(modelId, neuralFieldMap));
        return searchRequest;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String DEFAULT_MODEL_ID = "default_model_id";
        private static final String NEURAL_FIELD_DEFAULT_ID = "neural_field_default_id";

        @Override
        public NeuralQueryProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String modelId = (String) config.remove(DEFAULT_MODEL_ID);
            Map<String, Object> neuralInfoMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, NEURAL_FIELD_DEFAULT_ID);

            if (modelId == null && neuralInfoMap == null) {
                throw new IllegalArgumentException("model Id or neural info map either of them should be provided");
            }

            return new NeuralQueryProcessor(tag, description, ignoreFailure, modelId, neuralInfoMap);
        }
    }
}
