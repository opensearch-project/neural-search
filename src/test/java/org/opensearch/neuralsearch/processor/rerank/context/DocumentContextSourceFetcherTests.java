/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class DocumentContextSourceFetcherTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testContextFromSearchHit_whenFlatField_thenExtractsValue() {
        DocumentContextSourceFetcher fetcher = new DocumentContextSourceFetcher(List.of("title"));
        SearchResponse response = createSearchResponse(createHit("{\"title\": \"good morning\"}", "1"));

        fetchAndAssert(fetcher, response, contexts -> {
            assertEquals(1, contexts.size());
            assertEquals("good morning", contexts.get(0));
        });
    }

    @SneakyThrows
    public void testContextFromSearchHit_whenDotNotationField_thenExtractsValue() {
        DocumentContextSourceFetcher fetcher = new DocumentContextSourceFetcher(List.of("metadata.author"));
        SearchResponse response = createSearchResponse(createHit("{\"metadata\": {\"author\": \"John\"}}", "1"));

        fetchAndAssert(fetcher, response, contexts -> {
            assertEquals(1, contexts.size());
            assertEquals("John", contexts.get(0));
        });
    }

    @SneakyThrows
    public void testContextFromSearchHit_whenNestedArrayField_thenExtractsConcatenatedValues() {
        DocumentContextSourceFetcher fetcher = new DocumentContextSourceFetcher(List.of("content.text"));
        SearchResponse response = createSearchResponse(
            createHit("{\"content\": [{\"text\": \"good morning\", \"page\": 1}, {\"text\": \"good evening\", \"page\": 2}]}", "1")
        );

        fetchAndAssert(fetcher, response, contexts -> {
            assertEquals(1, contexts.size());
            assertTrue("Should contain 'good morning', got: " + contexts.get(0), contexts.get(0).contains("good morning"));
            assertTrue("Should contain 'good evening', got: " + contexts.get(0), contexts.get(0).contains("good evening"));
        });
    }

    @SneakyThrows
    public void testContextFromSearchHit_whenMissingField_thenReturnsEmpty() {
        DocumentContextSourceFetcher fetcher = new DocumentContextSourceFetcher(List.of("nonexistent"));
        SearchResponse response = createSearchResponse(createHit("{\"title\": \"hello\"}", "1"));

        fetchAndAssert(fetcher, response, contexts -> {
            assertEquals(1, contexts.size());
            assertEquals("", contexts.get(0));
        });
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private void fetchAndAssert(DocumentContextSourceFetcher fetcher, SearchResponse response, ContextAssertion assertion) {
        fetcher.fetchContext(new SearchRequest(), response, ActionListener.wrap(result -> {
            List<String> contexts = (List<String>) result.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD);
            assertion.assertContexts(contexts);
        }, e -> fail("Should not fail: " + e.getMessage())));
    }

    private interface ContextAssertion {
        void assertContexts(List<String> contexts);
    }

    private SearchHit createHit(String jsonSource, String id) {
        SearchHit hit = new SearchHit(Integer.parseInt(id), id, Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray(jsonSource));
        hit.score(1.0f);
        return hit;
    }

    private SearchResponse createSearchResponse(SearchHit... hits) {
        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new SearchResponse.Clusters(1, 1, 0), null);
    }
}
