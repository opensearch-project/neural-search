/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;

import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.visitor.NeuralSearchQueryVisitor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import lombok.Getter;
import lombok.Setter;

/**
 * Neural Search Query Request Processor, It modifies the search request with neural query clause
 * and adds model Id if not present in the search query.
 */
@Setter
@Getter
public class NeuralQueryEnricherProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "neural_query_enricher";

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

    private NeuralQueryEnricherProcessor(
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
         * @return NeuralQueryEnricherProcessor
         */
        @Override
        public NeuralQueryEnricherProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {
            String modelId = readOptionalStringProperty(TYPE, tag, config, DEFAULT_MODEL_ID);
            Map<String, Object> neuralInfoMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, NEURAL_FIELD_DEFAULT_ID);

            if (modelId == null && neuralInfoMap == null) {
                throw new IllegalArgumentException("model Id or neural info map either of them should be provided");
            }

            return new NeuralQueryEnricherProcessor(tag, description, ignoreFailure, modelId, neuralInfoMap);
        }
    }
}
