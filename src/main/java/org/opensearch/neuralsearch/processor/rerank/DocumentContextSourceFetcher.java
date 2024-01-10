/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ObjectPath;
import org.opensearch.search.SearchHit;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Context Source Fetcher that gets context from the search results (documents)
 */
@Log4j2
@AllArgsConstructor
public class DocumentContextSourceFetcher implements ContextSourceFetcher {

    public static final String NAME = "document_fields";
    public static final String DOCUMENT_CONTEXT_LIST_FIELD = "document_context_list";

    List<String> contextFields;

    /**
     * Fetch the information needed in order to rerank.
     * That could be as simple as grabbing a field from the search request or
     * as complicated as a lookup to some external service
     * @param searchRequest the search query
     * @param searchResponse the search results, in case they're relevant
     * @param listener be async
     */
    @Override
    public void fetchContext(SearchRequest searchRequest, SearchResponse searchResponse, ActionListener<Map<String, Object>> listener) {
        List<String> contexts = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits()) {
            StringBuilder ctx = new StringBuilder();
            for (String field : this.contextFields) {
                ctx.append(contextFromSearchHit(hit, field));
            }
            contexts.add(ctx.toString());
        }
        listener.onResponse(new HashMap<>(Map.of(DOCUMENT_CONTEXT_LIST_FIELD, contexts)));
    }

    private String contextFromSearchHit(final SearchHit hit, final String field) {
        if (hit.getFields().containsKey(field)) {
            Object fieldValue = hit.field(field).getValue();
            return String.valueOf(fieldValue);
        } else if (hit.hasSource() && hit.getSourceAsMap().containsKey(field)) {
            Object sourceValue = ObjectPath.eval(field, hit.getSourceAsMap());
            return String.valueOf(sourceValue);
        } else {
            log.warn(
                String.format(
                    Locale.ROOT,
                    "Could not find field %s in document %s for reranking! Using the empty string instead.",
                    field,
                    hit.getId()
                )
            );
            return "";
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create a document context source fetcher from list of field names provided by configuration
     * @param config configuration object grabbed from parsed API request. Should be a list of strings
     * @return a new DocumentContextSourceFetcher or throws IllegalArgumentException if config is malformed
     */
    public static DocumentContextSourceFetcher create(Object config) {
        if (!(config instanceof List)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be a list of field names", NAME));
        }
        List<?> fields = (List<?>) config;
        if (fields.size() == 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be nonempty", NAME));
        }
        List<String> fieldsAsStrings = fields.stream().map(field -> (String) field).collect(Collectors.toList());
        return new DocumentContextSourceFetcher(fieldsAsStrings);
    }
}
