/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.io.InputStream;
import java.util.Map;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

public class DefaultValueProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "default_query";

    public static final String MODEL_ID_FIELD = "model_id";
    public static final String NEURAL_FIELD_MAP_FIELD = "neural_field_map";

    final QueryBuilder queryBuilder;

    /**
     * Returns the type of the processor.
     *
     * @return The processor type.
     */
    @Override
    public String getType() {
        return TYPE;
    }


    protected DefaultValueProcessor(String tag, String description, boolean ignoreFailure, QueryBuilder neuralQueryBuilder) {
        super(tag, description, ignoreFailure);
        this.queryBuilder = neuralQueryBuilder;
    }

    @Override
    public SearchRequest processRequest(SearchRequest searchRequest) throws Exception {
        QueryBuilder originalQuery = null;
        if (searchRequest.source() != null) {
            originalQuery = searchRequest.source().query();
        }

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(originalQuery, MODEL_ID_FIELD, NEURAL_FIELD_MAP_FIELD);

        searchRequest.source().query(neuralQueryBuilder);

        return searchRequest;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String QUERY_KEY = "query";
        private final NamedXContentRegistry namedXContentRegistry;

        public Factory(NamedXContentRegistry namedXContentRegistry) {
            this.namedXContentRegistry = namedXContentRegistry;
        }

        @Override
        public DefaultValueProcessor create(
                Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
                String tag,
                String description,
                boolean ignoreFailure,
                Map<String, Object> config,
                PipelineContext pipelineContext
        ) throws Exception {
            Map<String, Object> query = ConfigurationUtils.readOptionalMap(TYPE, tag, config, QUERY_KEY);
            if (query == null) {
                throw new IllegalArgumentException("Did not specify the " + QUERY_KEY + " property in processor of type " + TYPE);
            }
            try (
                    XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(query);
                    InputStream stream = BytesReference.bytes(builder).streamInput();
                    XContentParser parser = MediaTypeRegistry.JSON.xContent()
                            .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                return new DefaultValueProcessor(tag, description, ignoreFailure, parseInnerQueryBuilder(parser));
            }
        }
    }

}
