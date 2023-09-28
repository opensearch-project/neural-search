/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.visitor.NeuralSearchQueryVisitor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

/**
 * Neural Search Query Request Processor, It modifies the search request with neural query clause
 * and adds model Id if not present in the search query.
 */
@Setter
@Getter
public class NeuralQueryProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "enriching_query_defaults";

    private final String modelId;

    private final Map<String, Object> neuralFieldDefaultIdMap;

    /**
     * Returns the type of the processor.
     *
     * @return The processor type.
     */
    @Override
    public String getType() {
        return TYPE;
    }

    private NeuralQueryProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        @Nullable String modelId,
        @Nullable Map<String, Object> neuralFieldDefaultIdMap
    ) {
        super(tag, description, ignoreFailure);
        this.modelId = modelId;
        this.neuralFieldDefaultIdMap = neuralFieldDefaultIdMap;
    }

    /**
     * Processes the Search Request.
     *
     * @return The Search Request.
     */
    @Override
    public SearchRequest processRequest(SearchRequest searchRequest) {
        QueryBuilder queryBuilder = searchRequest.source().query();
        queryBuilder.visit(new NeuralSearchQueryVisitor(modelId, neuralFieldDefaultIdMap));
        return searchRequest;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String DEFAULT_MODEL_ID = "default_model_id";
        private static final String NEURAL_FIELD_DEFAULT_ID = "neural_field_default_id";

        /**
         * Create the processor object.
         *
         * @return NeuralQueryProcessor
         */
        @Override
        public NeuralQueryProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {
            String modelId;
            try {
                modelId = (String) config.remove(DEFAULT_MODEL_ID);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("model Id must of String type");
            }
            Map<String, Object> neuralInfoMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, NEURAL_FIELD_DEFAULT_ID);

            if (modelId == null && neuralInfoMap == null) {
                throw new IllegalArgumentException("model Id or neural info map either of them should be provided");
            }

            return new NeuralQueryProcessor(tag, description, ignoreFailure, modelId, neuralInfoMap);
        }
    }
}
