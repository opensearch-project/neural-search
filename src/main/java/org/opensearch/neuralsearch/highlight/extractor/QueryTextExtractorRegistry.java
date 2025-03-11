/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.extractor;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;

import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for query text extractors that manages the extraction process
 */
@Log4j2
public class QueryTextExtractorRegistry {

    private final Map<Class<? extends Query>, QueryTextExtractor> extractors = new HashMap<>();
    private final GenericQueryTextExtractor fallbackExtractor = new GenericQueryTextExtractor();

    /**
     * Creates a new registry with default extractors
     */
    public QueryTextExtractorRegistry() {
        initialize();
    }

    /**
     * Initializes the registry with default extractors
     */
    public void initialize() {
        register(NeuralKNNQuery.class, new NeuralQueryTextExtractor());
        register(TermQuery.class, new TermQueryTextExtractor());

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
     * @throws IllegalArgumentException if the extracted query text is empty
     */
    public String extractQueryText(Query query, String fieldName) {
        log.debug("Extracting query text from query type: {}", query.getClass().getName());

        // Find the most specific extractor for this query type
        Class<?> queryClass = query.getClass();
        QueryTextExtractor extractor;

        // Look for an exact match first
        extractor = extractors.get(queryClass);

        // If no exact match, look for parent classes
        if (extractor == null) {
            for (Map.Entry<Class<? extends Query>, QueryTextExtractor> entry : extractors.entrySet()) {
                if (entry.getKey().isAssignableFrom(queryClass)) {
                    extractor = entry.getValue();
                    break;
                }
            }
        }

        // Use the extractor if found, otherwise use fallback
        String queryText;
        if (extractor != null) {
            queryText = extractor.extractQueryText(query, fieldName);
        } else {
            queryText = fallbackExtractor.extractQueryText(query, fieldName);
            log.info("Extracted query text using fallback extractor: {}", queryText);
        }

        if (queryText.isEmpty()) {
            throw new IllegalArgumentException("Query text is empty to perform neural highlighting");
        }

        return queryText;
    }
}
