/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Context Source Fetcher that gets context from the nested chunks returned as inner hits, one context per unique chunk
 */
@Log4j2
@AllArgsConstructor
public class InnerHitsContextSourceFetcher implements ContextSourceFetcher {

    public static final String NAME = "inner_hits";
    public static final String PATHS_FIELD = "paths";
    public static final String PATH_FIELD = "path";
    public static final String FIELD_FIELD = "field";
    /** Per top hit, the nested identities of its unique chunks, aligned with {@link DocumentContextSourceFetcher#DOCUMENT_CONTEXT_LIST_FIELD}. */
    public static final String CHUNK_IDENTITIES_FIELD = "inner_hits_chunk_identities";

    private final String path;
    private final String field;

    @Override
    public void fetchContext(
        final SearchRequest searchRequest,
        final SearchResponse searchResponse,
        final ActionListener<Map<String, Object>> listener
    ) {
        List<InnerHitBuilder> innerHits = innerHitsOnPath(searchRequest);
        if (innerHits != null && innerHits.isEmpty()) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "The query has no nested query on the path [%s] with inner hits, so [%s] cannot rerank chunks. "
                            + "Add \"inner_hits\": {} to the nested query on [%s].",
                        path,
                        NAME,
                        path
                    )
                )
            );
            return;
        }
        if (innerHits != null && innerHits.stream().noneMatch(this::returnsFieldInSource)) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "The inner hits on the nested path [%s] filter [%s.%s] out of _source, so [%s] cannot rerank chunks. "
                            + "Include it in the inner hits \"_source\".",
                        path,
                        path,
                        field,
                        NAME
                    )
                )
            );
            return;
        }
        List<String> contexts = new ArrayList<>();
        List<List<SearchHit.NestedIdentity>> chunkIdentities = new ArrayList<>();
        int missingFieldCount = 0;
        for (SearchHit hit : searchResponse.getHits()) {
            Map<SearchHit.NestedIdentity, SearchHit> chunkByIdentity = new LinkedHashMap<>();
            collectChunks(hit, chunkByIdentity);
            for (SearchHit chunk : chunkByIdentity.values()) {
                String text = chunkText(chunk);
                if (text == null) {
                    missingFieldCount++;
                }
                contexts.add(text);
            }
            chunkIdentities.add(new ArrayList<>(chunkByIdentity.keySet()));
        }
        if (missingFieldCount > 0) {
            log.debug(
                String.format(
                    Locale.ROOT,
                    "Field [%s] is missing in %d of %d chunks on the nested path [%s], they get no rerank score",
                    field,
                    missingFieldCount,
                    contexts.size(),
                    path
                )
            );
        }
        listener.onResponse(
            new HashMap<>(
                Map.of(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD, contexts, CHUNK_IDENTITIES_FIELD, chunkIdentities)
            )
        );
    }

    /**
     * Collects the unique chunks of the configured path from the hit's inner hits lists, descending into the inner hits of
     * other paths' inner hits, where chunks of a nested-in-nested path end up when both levels request inner hits.
     */
    private void collectChunks(final SearchHit hit, final Map<SearchHit.NestedIdentity, SearchHit> chunkByIdentity) {
        if (hit.getInnerHits() == null) {
            return;
        }
        for (SearchHits list : hit.getInnerHits().values()) {
            SearchHit[] innerHits = list.getHits();
            if (innerHits.length == 0) {
                continue;
            }
            boolean onPath = path.equals(nestedPath(innerHits[0].getNestedIdentity()));
            for (SearchHit innerHit : innerHits) {
                if (onPath) {
                    chunkByIdentity.putIfAbsent(innerHit.getNestedIdentity(), innerHit);
                } else {
                    collectChunks(innerHit, chunkByIdentity);
                }
            }
        }
    }

    /**
     * The inner hits of the nested queries on the configured path, or null when the request has no query to inspect.
     */
    private List<InnerHitBuilder> innerHitsOnPath(final SearchRequest searchRequest) {
        if (searchRequest == null || searchRequest.source() == null || searchRequest.source().query() == null) {
            return null;
        }
        List<InnerHitBuilder> innerHits = new ArrayList<>();
        searchRequest.source().query().visit(new QueryBuilderVisitor() {
            @Override
            public void accept(QueryBuilder query) {
                if (query instanceof NestedQueryBuilder nested && path.equals(nested.path()) && nested.innerHit() != null) {
                    innerHits.add(nested.innerHit());
                }
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return this;
            }
        });
        return innerHits;
    }

    /** Whether the inner hits' _source filtering keeps the rerank field, checked by filtering a source that holds just that field. */
    private boolean returnsFieldInSource(final InnerHitBuilder innerHit) {
        FetchSourceContext sourceContext = innerHit.getFetchSourceContext();
        if (sourceContext == null) {
            return true;
        }
        if (!sourceContext.fetchSource()) {
            return false;
        }
        // Inner hits filter the root source, so the field is addressed by its full path
        String fullField = path + "." + field;
        String[] parts = fullField.split("\\.");
        Map<String, Object> source = Map.of(parts[parts.length - 1], "probe");
        for (int i = parts.length - 2; i >= 0; i--) {
            source = Map.of(parts[i], source);
        }
        return XContentMapValues.extractValue(fullField, sourceContext.getFilter().apply(source)) != null;
    }

    /** Joins the relative field names of a (possibly multi-level) nested identity into the full nested path. */
    private static String nestedPath(SearchHit.NestedIdentity identity) {
        if (identity == null) {
            return null;
        }
        StringBuilder fullPath = new StringBuilder(identity.getField().string());
        for (SearchHit.NestedIdentity child = identity.getChild(); child != null; child = child.getChild()) {
            fullPath.append('.').append(child.getField().string());
        }
        return fullPath.toString();
    }

    /**
     * Reads the chunk text from the inner hit's own source, falling back to the full field path (as in semantic highlighting).
     * Returns null when the field is missing.
     */
    private String chunkText(final SearchHit innerHit) {
        Map<String, Object> source = innerHit.hasSource() ? innerHit.getSourceAsMap() : Map.of();
        Object value = XContentMapValues.extractValue(field, source);
        if (value == null) {
            value = XContentMapValues.extractValue(path + "." + field, source);
        }
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> values) {
            return values.stream().map(String::valueOf).collect(Collectors.joining(" "));
        }
        return String.valueOf(value);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create an inner hits context source fetcher from the {@code inner_hits} configuration
     * @param config configuration object grabbed from parsed API request. Should be a map with a single-entry {@code paths} list
     * @return a new InnerHitsContextSourceFetcher or throws IllegalArgumentException if config is malformed
     */
    public static InnerHitsContextSourceFetcher create(Object config) {
        if (!(config instanceof Map<?, ?> map) || !(map.get(PATHS_FIELD) instanceof List<?> paths)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s must be an object with a [%s] list of {%s, %s} entries",
                    NAME,
                    PATHS_FIELD,
                    PATH_FIELD,
                    FIELD_FIELD
                )
            );
        }
        // ponytail: one path until combining scores across paths (score_mode) is needed
        if (paths.size() != 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s.%s must contain exactly one entry, got %d", NAME, PATHS_FIELD, paths.size())
            );
        }
        if (!(paths.get(0) instanceof Map<?, ?> entry)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s.%s entries must be objects with [%s] and [%s]", NAME, PATHS_FIELD, PATH_FIELD, FIELD_FIELD)
            );
        }
        return new InnerHitsContextSourceFetcher(requireText(entry, PATH_FIELD), requireText(entry, FIELD_FIELD));
    }

    private static String requireText(Map<?, ?> entry, String key) {
        if (!(entry.get(key) instanceof String value) || value.isBlank()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s.%s entries must set [%s] to a non-empty string", NAME, PATHS_FIELD, key)
            );
        }
        return value;
    }
}
