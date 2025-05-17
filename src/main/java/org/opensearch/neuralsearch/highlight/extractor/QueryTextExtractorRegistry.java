/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.neuralsearch.query.HybridQuery;

import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for query text extractors that manages the extraction process
 */
@Log4j2
public class QueryTextExtractorRegistry {

    private final Map<Class<? extends Query>, QueryTextExtractor> extractors = new HashMap<>();

    /**
     * Creates a new registry with default extractors
     */
    public QueryTextExtractorRegistry() {
        initialize();
    }

    /**
     * Initializes the registry with default extractors
     */
    private void initialize() {
        register(NeuralKNNQuery.class, new NeuralQueryTextExtractor());
        register(TermQuery.class, new TermQueryTextExtractor());
        register(HybridQuery.class, new HybridQueryTextExtractor(this));

        // Handle nested query
        register(OpenSearchToParentBlockJoinQuery.class, new NestedQueryTextExtractor(this));

        // BooleanQueryTextExtractor needs a reference to this registry
        // so we need to register it after creating the registry instance
        register(BooleanQuery.class, new BooleanQueryTextExtractor(this));
    }

    /**
     * Registers an extractor for a specific query type
     *
     * @param queryClass The query class to register for
     * @param extractor The extractor to use for this query type
     */
    public <T extends Query> void register(Class<T> queryClass, QueryTextExtractor extractor) {
        extractors.put(queryClass, extractor);
    }

    /**
     * Extracts text from a query using the appropriate extractor
     *
     * @param query The query to extract text from
     * @param fieldName The name of the field being highlighted
     * @return The extracted query text
     */
    public String extractQueryText(Query query, String fieldName) {
        if (query == null) {
            log.warn("Cannot extract text from null query");
            return null;
        }
        Class<?> queryClass = query.getClass();
        QueryTextExtractor extractor;

        extractor = extractors.get(queryClass);

        if (extractor == null) {
            log.warn("No extractor found for query type: {}", queryClass.getName());
            return null;
        }

        return extractor.extractQueryText(query, fieldName);
    }
}
