/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Getter;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.Nullable;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;

/**
 * Query enricher that will populate the model_id option when the semantic highlighter is used and no model_id is
 * specified in the search query body.
 */
@Getter
public class SemanticHighlighterQueryEnricherProcessor extends AbstractProcessor implements SearchRequestProcessor {
    public static final String TYPE = SemanticHighlightingConstants.QUERY_ENRICHER_TYPE;

    private final String modelId;
    private final Map<String, Object> fieldDefaultIdMap;

    private SemanticHighlighterQueryEnricherProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        @Nullable String modelId,
        @Nullable Map<String, Object> fieldDefaultIdMap
    ) {
        super(tag, description, ignoreFailure);
        this.modelId = modelId;
        this.fieldDefaultIdMap = fieldDefaultIdMap;
    }

    @Override
    public SearchRequest processRequest(SearchRequest searchRequest) {
        EventStatsManager.increment(EventStatName.SEMANTIC_HIGHLIGHTING_QUERY_ENRICHER_EXECUTIONS);
        Optional<SearchSourceBuilder> source = Optional.ofNullable(searchRequest.source());
        source.map(SearchSourceBuilder::highlighter).ifPresent(this::enrichHighlight);
        source.map(SearchSourceBuilder::query).ifPresent(qb -> qb.visit(new NestedQueryHighlightVisitor()));
        // NOTE: we explicitly do not enrich TopHitsAggregationBuilder highlighters because it is not useful yet — the batch path ignores
        // aggregations entirely (HighlightConfigResolver only walks source.highlighter() and inner_hits, HighlightContextBuilder only reads
        // response.getHits()), so under ext.semantic_highlighting_batch the model_id would be set but never used, and highlights would go
        // missing silently. Needs the highlighting feature to support aggregations first.
        return searchRequest;
    }

    private void enrichHighlight(HighlightBuilder hlBuilder) {
        Map<String, Object> globalOptions = hlBuilder.options();
        boolean userSuppliedGlobalModelId = globalOptions != null && globalOptions.containsKey(SemanticHighlightingConstants.MODEL_ID);
        if (userSuppliedGlobalModelId) {
            // if the user provided a global model_id there's no need to enrich anything.
            return;
        }

        boolean globalIsSemantic = false;
        if (SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(hlBuilder.highlighterType())) {
            globalIsSemantic = true;
            if (modelId != null) {
                hlBuilder.options(enrichWithModelId(globalOptions, modelId));
            }
        }
        for (HighlightBuilder.Field field : Optional.ofNullable(hlBuilder.fields()).orElseGet(Collections::emptyList)) {
            // Enrich if either:
            // - the global type is semantic and the field specific type is unset
            // - the field specific type is set to semantic
            if ((globalIsSemantic && field.highlighterType() == null)
                || SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType())) {
                String fieldModelId = (String) Optional.ofNullable(this.fieldDefaultIdMap)
                    .orElseGet(Collections::emptyMap)
                    .getOrDefault(field.name(), modelId);
                if (fieldModelId != null) {
                    field.options(enrichWithModelId(field.options(), fieldModelId));
                }
                // else: no default model_id and no per-field override for this field, nothing to enrich
            }
        }
    }

    private Map<String, Object> enrichWithModelId(@Nullable Map<String, Object> options, String modelId) {
        if (options != null && options.containsKey(SemanticHighlightingConstants.MODEL_ID)) {
            return options;
        }
        Map<String, Object> enrichedOptions = options != null ? new HashMap<>(options) : new HashMap<>();
        enrichedOptions.put(SemanticHighlightingConstants.MODEL_ID, modelId);
        return enrichedOptions;
    }

    private class NestedQueryHighlightVisitor implements QueryBuilderVisitor {
        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof NestedQueryBuilder nested) {
                Optional.ofNullable(nested.innerHit())
                    .map(InnerHitBuilder::getHighlightBuilder)
                    .ifPresent(SemanticHighlighterQueryEnricherProcessor.this::enrichHighlight);
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
        }
    }

    @Override
    public String getType() {
        return SemanticHighlightingConstants.QUERY_ENRICHER_TYPE;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String DEFAULT_MODEL_ID = "default_model_id";
        private static final String SEMANTIC_HIGHLIGHTER_FIELD_DEFAULT_ID = "semantic_highlighter_field_default_id";

        /**
         * Create the processor object.
         *
         * @return {@link SemanticHighlighterQueryEnricherProcessor}
         */
        @Override
        public SemanticHighlighterQueryEnricherProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {
            String modelId = readOptionalStringProperty(TYPE, tag, config, DEFAULT_MODEL_ID);
            Map<String, Object> fieldMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, SEMANTIC_HIGHLIGHTER_FIELD_DEFAULT_ID);

            if (modelId == null && fieldMap == null) {
                throw new IllegalArgumentException("[default_model_id] or [semantic_highlighter_field_default_id] should be provided");
            }

            if (fieldMap != null) {
                List<String> nonStringFields = fieldMap.entrySet()
                    .stream()
                    .filter(en -> !(en.getValue() instanceof String))
                    .map(Map.Entry::getKey)
                    .toList();
                if (!nonStringFields.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Invalid type in [semantic_highlighter_field_default_id]: value for ["
                            + String.join(", ", nonStringFields)
                            + "] must be a model_id of type string"
                    );
                }
            }

            return new SemanticHighlighterQueryEnricherProcessor(
                tag,
                description,
                ignoreFailure,
                modelId,
                fieldMap != null ? Collections.unmodifiableMap(fieldMap) : null
            );
        }
    }
}
