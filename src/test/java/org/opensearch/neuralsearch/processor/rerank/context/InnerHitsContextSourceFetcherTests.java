/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
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

    public void testFetchContext_whenFieldMissingInChunks_thenContextIsNull() {
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

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("chunks", "text"), createSearchResponse(parent));

        assertEquals(Arrays.asList(null, "beta", null), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
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

    public void testFetchContext_whenChunksAreInnerHitsOfInnerHits_thenCollectsThem() {
        SearchHit.NestedIdentity firstChunk = new SearchHit.NestedIdentity("sections", 0, identity("chunks", 0));
        SearchHit.NestedIdentity secondChunk = new SearchHit.NestedIdentity("sections", 1, identity("chunks", 2));
        SearchHit firstSection = createInnerHit(identity("sections", 0), "{\"title\": \"one\"}");
        firstSection.setInnerHits(Map.of("sections.chunks", innerHitsList(createInnerHit(firstChunk, "{\"text\": \"alpha\"}"))));
        SearchHit secondSection = createInnerHit(identity("sections", 1), "{\"title\": \"two\"}");
        secondSection.setInnerHits(Map.of("sections.chunks", innerHitsList(createInnerHit(secondChunk, "{\"text\": \"beta\"}"))));
        SearchHit parent = createHit("1");
        parent.setInnerHits(Map.of("sections", innerHitsList(firstSection, secondSection)));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("sections.chunks", "text"), createSearchResponse(parent));

        assertEquals(List.of("alpha", "beta"), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
        assertEquals(List.of(List.of(firstChunk, secondChunk)), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
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
    }

    public void testFetchContext_whenNestedQueryHasNoInnerHits_thenFail() {
        assertMissingInnerHitsFailure(nested("embeddings", false));
    }

    public void testFetchContext_whenInnerHitsOnlyOnOtherPath_thenFail() {
        assertMissingInnerHitsFailure(QueryBuilders.boolQuery().should(nested("comments", true)).should(nested("embeddings", false)));
    }

    public void testFetchContext_whenQueryHasNoNestedQuery_thenFail() {
        assertMissingInnerHitsFailure(QueryBuilders.matchAllQuery());
    }

    public void testFetchContext_whenInnerHitsInsideCompoundQuery_thenSucceeds() {
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.constantScoreQuery(QueryBuilders.disMaxQuery().add(nested("embeddings", true))));

        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("embeddings", "text"),
            requestWithQuery(query),
            createSearchResponse(createHit("1"))
        );

        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    public void testFetchContext_whenInnerHitsOnNestedInNestedPath_thenSucceeds() {
        NestedQueryBuilder chunks = nested("sections.chunks", true);
        QueryBuilder query = QueryBuilders.nestedQuery("sections", chunks, ScoreMode.Max);

        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("sections.chunks", "text"),
            requestWithQuery(query),
            createSearchResponse(createHit("1"))
        );

        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    public void testFetchContext_whenRequestHasNoQuery_thenSucceeds() {
        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("embeddings", "text"),
            new SearchRequest().source(new SearchSourceBuilder()),
            createSearchResponse(createHit("1"))
        );

        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    public void testFetchContext_whenInnerHitsDisableSource_thenFail() {
        assertSourceFilterFailure(nested("embeddings", new FetchSourceContext(false)));
    }

    public void testFetchContext_whenInnerHitsSourceIncludesOtherField_thenFail() {
        assertSourceFilterFailure(nested("embeddings", new FetchSourceContext(true, new String[] { "embeddings.other" }, null)));
    }

    public void testFetchContext_whenInnerHitsSourceExcludesField_thenFail() {
        assertSourceFilterFailure(nested("embeddings", new FetchSourceContext(true, null, new String[] { "embeddings.te*" })));
    }

    public void testFetchContext_whenInnerHitsSourceIncludesField_thenSucceeds() {
        for (String include : List.of("embeddings", "embeddings.*", "embeddings.text", "*")) {
            QueryBuilder query = nested("embeddings", new FetchSourceContext(true, new String[] { include }, null));

            Map<String, Object> context = fetch(
                new InnerHitsContextSourceFetcher("embeddings", "text"),
                requestWithQuery(query),
                createSearchResponse(createHit("1"))
            );

            assertEquals(include, List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
        }
    }

    public void testFetchContext_whenOneOfSeveralInnerHitsReturnsField_thenSucceeds() {
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(nested("embeddings", new FetchSourceContext(false)))
            .should(nested("embeddings", new FetchSourceContext(true, new String[] { "embeddings.text" }, null)));

        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("embeddings", "text"),
            requestWithQuery(query),
            createSearchResponse(createHit("1"))
        );

        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    public void testFetchContext_whenNestedInNestedSourceIncludesDottedField_thenSucceeds() {
        QueryBuilder query = QueryBuilders.nestedQuery(
            "sections",
            nested("sections.chunks", new FetchSourceContext(true, new String[] { "sections.chunks.meta.text" }, null)),
            ScoreMode.Max
        );

        Map<String, Object> context = fetch(
            new InnerHitsContextSourceFetcher("sections.chunks", "meta.text"),
            requestWithQuery(query),
            createSearchResponse(createHit("1"))
        );

        assertEquals(List.of(List.of()), context.get(InnerHitsContextSourceFetcher.CHUNK_IDENTITIES_FIELD));
    }

    private void assertSourceFilterFailure(QueryBuilder query) {
        List<Exception> failures = new ArrayList<>();
        new InnerHitsContextSourceFetcher("embeddings", "text").fetchContext(
            requestWithQuery(query),
            createSearchResponse(createHit("1")),
            ActionListener.wrap(result -> fail("Should not succeed"), failures::add)
        );

        assertEquals(1, failures.size());
        assertTrue(failures.get(0) instanceof IllegalArgumentException);
        assertEquals(
            "The inner hits on the nested path [embeddings] filter [embeddings.text] out of _source, so [inner_hits] "
                + "cannot rerank chunks. Include it in the inner hits \"_source\".",
            failures.get(0).getMessage()
        );
    }

    private static NestedQueryBuilder nested(String path, FetchSourceContext sourceContext) {
        return QueryBuilders.nestedQuery(path, QueryBuilders.matchAllQuery(), ScoreMode.Max)
            .innerHit(new InnerHitBuilder().setFetchSourceContext(sourceContext));
    }

    private void assertMissingInnerHitsFailure(QueryBuilder query) {
        List<Exception> failures = new ArrayList<>();
        new InnerHitsContextSourceFetcher("embeddings", "text").fetchContext(
            requestWithQuery(query),
            createSearchResponse(createHit("1")),
            ActionListener.wrap(result -> fail("Should not succeed"), failures::add)
        );

        assertEquals(1, failures.size());
        assertTrue(failures.get(0) instanceof IllegalArgumentException);
        assertEquals(
            "The query has no nested query on the path [embeddings] with inner hits, so [inner_hits] "
                + "cannot rerank chunks. Add \"inner_hits\": {} to the nested query on [embeddings].",
            failures.get(0).getMessage()
        );
    }

    private static NestedQueryBuilder nested(String path, boolean innerHits) {
        NestedQueryBuilder nested = QueryBuilders.nestedQuery(path, QueryBuilders.matchAllQuery(), ScoreMode.Max);
        return innerHits ? nested.innerHit(new InnerHitBuilder()) : nested;
    }

    private static SearchRequest requestWithQuery(QueryBuilder query) {
        return new SearchRequest().source(new SearchSourceBuilder().query(query));
    }

    public void testFetchContext_whenInnerHitsListsAreEmpty_thenSucceedsWithoutChunks() {
        SearchHit hit = createHit("1");
        hit.setInnerHits(Map.of("chunks", innerHitsList()));

        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse(hit));

        assertEquals(List.of(), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
    }

    public void testFetchContext_whenNoHits_thenSucceedsEmpty() {
        Map<String, Object> context = fetch(new InnerHitsContextSourceFetcher("embeddings", "text"), createSearchResponse());

        assertEquals(List.of(), context.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD));
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
        return fetch(fetcher, new SearchRequest(), response);
    }

    @SneakyThrows
    private Map<String, Object> fetch(InnerHitsContextSourceFetcher fetcher, SearchRequest request, SearchResponse response) {
        List<Map<String, Object>> captured = new ArrayList<>();
        fetcher.fetchContext(request, response, ActionListener.wrap(captured::add, e -> fail("Should not fail: " + e)));
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
