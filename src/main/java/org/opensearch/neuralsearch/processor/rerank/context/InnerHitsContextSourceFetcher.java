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

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

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
    /** Per top hit, the names of the inner hits lists that hold chunks of the configured nested path. */
    public static final String LIST_NAMES_FIELD = "inner_hits_list_names";

    private final String path;
    private final String field;

    @Override
    public void fetchContext(
        final SearchRequest searchRequest,
        final SearchResponse searchResponse,
        final ActionListener<Map<String, Object>> listener
    ) {
        List<String> contexts = new ArrayList<>();
        List<List<SearchHit.NestedIdentity>> chunkIdentities = new ArrayList<>();
        List<List<String>> listNames = new ArrayList<>();
        boolean anyMatchingList = false;
        // OpenSearch returns an empty list for every inner_hits name even when a hit has no nested matches, so an empty
        // list proves the query requests inner hits; its path is unknown, though
        boolean anyEmptyList = false;
        int missingFieldCount = 0;
        for (SearchHit hit : searchResponse.getHits()) {
            Map<SearchHit.NestedIdentity, SearchHit> chunkByIdentity = new LinkedHashMap<>();
            List<String> matchingLists = new ArrayList<>();
            Map<String, SearchHits> inners = hit.getInnerHits() == null ? Map.of() : hit.getInnerHits();
            for (Map.Entry<String, SearchHits> entry : inners.entrySet()) {
                SearchHit[] innerHitArray = entry.getValue().getHits();
                if (innerHitArray.length == 0) {
                    anyEmptyList = true;
                    continue;
                }
                if (!path.equals(nestedPath(innerHitArray[0].getNestedIdentity()))) {
                    continue;
                }
                matchingLists.add(entry.getKey());
                for (SearchHit innerHit : innerHitArray) {
                    chunkByIdentity.putIfAbsent(innerHit.getNestedIdentity(), innerHit);
                }
            }
            anyMatchingList |= !matchingLists.isEmpty();
            for (SearchHit chunk : chunkByIdentity.values()) {
                String text = chunkText(chunk);
                if (text == null) {
                    missingFieldCount++;
                    text = "";
                }
                contexts.add(text);
            }
            chunkIdentities.add(new ArrayList<>(chunkByIdentity.keySet()));
            listNames.add(matchingLists);
        }
        boolean innerHitsMissingFromQuery = !anyMatchingList && !anyEmptyList && searchResponse.getHits().getHits().length > 0;
        if (innerHitsMissingFromQuery) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No inner hits on the nested path [%s] found in the search results, so [%s] cannot rerank chunks. "
                            + "Add \"inner_hits\": {} to the nested query on [%s].",
                        path,
                        NAME,
                        path
                    )
                )
            );
            return;
        }
        if (missingFieldCount > 0) {
            log.warn(
                String.format(
                    Locale.ROOT,
                    "Field [%s] is missing in %d of %d chunks on the nested path [%s], reranking them with an empty string",
                    field,
                    missingFieldCount,
                    contexts.size(),
                    path
                )
            );
        }
        listener.onResponse(
            new HashMap<>(
                Map.of(
                    DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD,
                    contexts,
                    CHUNK_IDENTITIES_FIELD,
                    chunkIdentities,
                    LIST_NAMES_FIELD,
                    listNames
                )
            )
        );
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
