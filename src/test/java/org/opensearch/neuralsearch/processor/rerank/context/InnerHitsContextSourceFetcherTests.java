/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class InnerHitsContextSourceFetcherTests extends OpenSearchTestCase {

    public void testFetchContext_whenChunkInSeveralLists_thenDeduplicatesChunks() {
        SearchHit parent = createHit("1");
        Map<String, SearchHits> lists = new LinkedHashMap<>();
        lists.put("keyword_chunks", innerHitsList(createInnerHit(identity("embeddings", 0), "{\"text\": \"alpha\"}")));
        lists.put(
            "semantic_chunks",
            innerHitsList(
                createInnerHit(identity("embeddings", 0), "{\"text\": \"alpha\"}"),
                createInnerHit(identity("embeddings", 1), "{\"text\": \"beta\"}")
            )
        );
        parent.setInnerHits(lists);

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse(parent));

        assertEquals(List.of("alpha", "beta"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(
            List.of(List.of(identity("embeddings", 0), identity("embeddings", 1))),
            context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD)
        );
        assertEquals(List.of(List.of("keyword_chunks", "semantic_chunks")), context.get(InnerHitsContextSourceFetcher.LIST_NAMES_FIELD));
    }

    public void testFetchContext_whenFieldIsInsideObject_thenExtractsValue() {
        SearchHit parent = createHit("1");
        parent.setInnerHits(Map.of("chunks", innerHitsList(createInnerHit(identity("chunks", 0), "{\"meta\": {\"text\": \"alpha\"}}"))));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("chunks", "meta.text"), createSearchResponse(parent));

        assertEquals(List.of("alpha"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
    }

    public void testFetchContext_whenFieldIsMultiValued_thenJoinsValues() {
        SearchHit parent = createHit("1");
        parent.setInnerHits(Map.of("chunks", innerHitsList(createInnerHit(identity("chunks", 0), "{\"text\": [\"alpha\", \"beta\"]}"))));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("chunks", "text"), createSearchResponse(parent));

        assertEquals(List.of("alpha beta"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
    }

    public void testFetchContext_whenFieldMissingInChunks_thenUsesEmptyStringAndWarnsOnce() throws IllegalAccessException {
        SearchHit parent = createHit("1");
        parent.setInnerHits(
            Map.of(
                "chunks",
                innerHitsList(
                    createInnerHit(identity("chunks", 0), "{\"other\": \"alpha\"}"),
                    createInnerHit(identity("chunks", 1), "{\"text\": \"beta\"}"),
                    createInnerHit(identity("chunks", 2), "{}")
                )
            )
        );
        String loggerName = InnerHitsContextSourceFetcher.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(loggerName))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "aggregated warning",
                    loggerName,
                    Level.WARN,
                    "Field [text] is missing in 2 of 3 chunks on the nested path [chunks]*"
                )
            );

            Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("chunks", "text"), createSearchResponse(parent));

            assertEquals(List.of("", "beta", ""), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFetchContext_whenNestedInNested_thenMatchesFullPathAndKeepsChunksApart() {
        SearchHit parent = createHit("1");
        SearchHit.NestedIdentity firstSection = new SearchHit.NestedIdentity("sections", 0, identity("chunks", 0));
        SearchHit.NestedIdentity secondSection = new SearchHit.NestedIdentity("sections", 1, identity("chunks", 0));
        parent.setInnerHits(
            Map.of(
                "chunks",
                innerHitsList(createInnerHit(firstSection, "{\"text\": \"alpha\"}"), createInnerHit(secondSection, "{\"text\": \"beta\"}"))
            )
        );

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("sections.chunks", "text"), createSearchResponse(parent));

        assertEquals(List.of("alpha", "beta"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(List.of(List.of(firstSection, secondSection)), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    public void testFetchContext_whenSourceUsesFullFieldPath_thenExtractsValue() {
        SearchHit parent = createHit("1");
        parent.setInnerHits(Map.of("chunks", innerHitsList(createInnerHit(identity("embeddings", 0), "{\"embeddings.text\": \"alpha\"}"))));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse(parent));

        assertEquals(List.of("alpha"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
    }

    public void testFetchContext_whenInnerHitsOnOtherPath_thenHitHasNoChunks() {
        SearchHit matching = createHit("1");
        matching.setInnerHits(Map.of("chunks", innerHitsList(createInnerHit(identity("embeddings", 0), "{\"text\": \"alpha\"}"))));
        SearchHit other = createHit("2");
        other.setInnerHits(Map.of("comments", innerHitsList(createInnerHit(identity("comments", 0), "{\"text\": \"beta\"}"))));
        SearchHit bare = createHit("3");

        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("embeddings", "text"),
            createSearchResponse(matching, other, bare)
        );

        assertEquals(List.of("alpha"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(
            List.of(List.of(identity("embeddings", 0)), List.of(), List.of()),
            context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD)
        );
        assertEquals(List.of(List.of("chunks"), List.of(), List.of()), context.get(InnerHitsContextSourceFetcher.LIST_NAMES_FIELD));
    }

    public void testFetchContext_whenNoHitHasMatchingList_thenFail() {
        InnerHitsContextSourceFetcher fetcher = new InnerHitsContextSourceFetcher("embeddings", "text");

        List<Exception> failures = new ArrayList<>();
        fetcher.fetchContext(
            new SearchRequest(),
            createSearchResponse(createHit("1")),
            ActionListener.wrap(result -> fail("Should not succeed"), failures::add)
        );

        assertEquals(1, failures.size());
        assertTrue(failures.get(0) instanceof IllegalArgumentException);
        assertEquals(
            "No inner hits on the nested path [embeddings] found in the search results, so [inner_hits] "
                + "cannot rerank chunks. Add \"inner_hits\": {} to the nested query on [embeddings].",
            failures.get(0).getMessage()
        );
    }

    public void testFetchContext_whenInnerHitsListsAreEmpty_thenSucceedsWithoutChunks() {
        SearchHit hit = createHit("1");
        hit.setInnerHits(Map.of("chunks", innerHitsList()));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse(hit));

        assertEquals(List.of(), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.LIST_NAMES_FIELD));
    }

    public void testFetchContext_whenNoHits_thenSucceedsEmpty() {
        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse());

        assertEquals(List.of(), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(List.of(), context.get(InnerHitsContextSourceFetcher.LIST_NAMES_FIELD));
    }

    public void testCreate_whenOnePath_thenSuccessful() {
        InnerHitsContextSourceFetcher fetcher = InnerHitsContextSourceFetcher.create(
            config(List.of(Map.of("path", "chunks", "field", "meta.text")))
        );

        assertEquals(InnerHitsContextSourceFetcher.NAME, fetcher.getName());
    }

    public void testCreate_whenTwoPaths_thenFail() {
        Object config = config(List.of(Map.of("path", "chunks", "field", "text"), Map.of("path", "comments", "field", "text")));

        assertEquals(
            "inner_hits.paths must contain exactly one entry, got 2",
            assertThrows(IllegalArgumentException.class, () -> InnerHitsContextSourceFetcher.create(config)).getMessage()
        );
    }

    public void testCreate_whenNoPaths_thenFail() {
        assertEquals(
            "inner_hits.paths must contain exactly one entry, got 0",
            assertThrows(IllegalArgumentException.class, () -> InnerHitsContextSourceFetcher.create(config(List.of()))).getMessage()
        );
    }

    public void testCreate_whenNotAnObject_thenFail() {
        assertEquals(
            "inner_hits must be an object with a [paths] list of {path, field} entries",
            assertThrows(IllegalArgumentException.class, () -> InnerHitsContextSourceFetcher.create(true)).getMessage()
        );
    }

    public void testCreate_whenFieldMissing_thenFail() {
        assertEquals(
            "inner_hits.paths entries must set [field] to a non-empty string",
            assertThrows(
                IllegalArgumentException.class,
                () -> InnerHitsContextSourceFetcher.create(config(List.of(Map.of("path", "chunks"))))
            ).getMessage()
        );
    }

    public void testCreate_whenPathBlank_thenFail() {
        Object config = config(List.of(Map.of("path", " ", "field", "text")));

        assertEquals(
            "inner_hits.paths entries must set [path] to a non-empty string",
            assertThrows(IllegalArgumentException.class, () -> InnerHitsContextSourceFetcher.create(config)).getMessage()
        );
    }

    private Object config(List<?> paths) {
        return Map.of(InnerHitsContextSourceFetcher.PATHS_FIELD, paths);
    }

    @SneakyThrows
    private Map<String, Object> fetch(InnerHitsContextSourceFetcher fetcher, SearchResponse response) {
        List<Map<String, Object>> captured = new ArrayList<>();
        fetcher.fetchContext(new SearchRequest(), response, ActionListener.wrap(captured::add, e -> fail("Should not fail: " + e)));
        assertEquals(1, captured.size());
        return captured.get(0);
    }

    private SearchHit.NestedIdentity identity(String field, int offset) {
        return new SearchHit.NestedIdentity(field, offset, null);
    }

    private SearchHit createInnerHit(SearchHit.NestedIdentity identity, String jsonSource) {
        SearchHit hit = new SearchHit(
            identity.getOffset(),
            "chunk-" + identity.getOffset(),
            identity,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        hit.sourceRef(new BytesArray(jsonSource));
        return hit;
    }

    private SearchHits innerHitsList(SearchHit... hits) {
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
    }

    private SearchHit createHit(String id) {
        SearchHit hit = new SearchHit(Integer.parseInt(id), id, Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"title\": \"doc\"}"));
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
