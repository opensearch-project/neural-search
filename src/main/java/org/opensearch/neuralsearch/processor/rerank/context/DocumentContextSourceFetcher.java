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
import java.util.Optional;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.RERANKER_MAX_DOC_FIELDS;

import org.opensearch.neuralsearch.processor.util.ProcessorUtils;

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
    public static final String INNER_HITS_FIELD = "inner_hits";
    /** Per top hit, the nested offsets of its unique chunks, aligned with {@link #DOCUMENT_CONTEXT_LIST_FIELD}. */
    public static final String INNER_HITS_CHUNK_OFFSETS_FIELD = "document_inner_hits_chunk_offsets";
    /** Per top hit, the names of the inner hits lists that hold chunks of the configured nested path. */
    public static final String INNER_HITS_LIST_NAMES_FIELD = "document_inner_hits_list_names";
    public static final String INNER_HITS_REQUIREMENT_MESSAGE = String.format(
        Locale.ROOT,
        "%s requires exactly one %s entry naming a nested field as <nested_path>.<leaf>",
        INNER_HITS_FIELD,
        NAME
    );

    private final List<String> contextFields;
    private final boolean innerHits;

    /**
     * Fetch the information needed in order to rerank.
     * That could be as simple as grabbing a field from the search request or
     * as complicated as a lookup to some external service
     * @param searchRequest the search query
     * @param searchResponse the search results, in case they're relevant
     * @param listener be async
     */
    @Override
    public void fetchContext(
        final SearchRequest searchRequest,
        final SearchResponse searchResponse,
        final ActionListener<Map<String, Object>> listener
    ) {
        if (innerHits) {
            fetchInnerHitsContext(searchResponse, listener);
            return;
        }
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

    private void fetchInnerHitsContext(final SearchResponse searchResponse, final ActionListener<Map<String, Object>> listener) {
        String field = contextFields.get(0);
        String nestedPath = field.substring(0, field.lastIndexOf('.'));
        String leafField = field.substring(field.lastIndexOf('.') + 1);

        List<String> contexts = new ArrayList<>();
        List<List<Integer>> chunkOffsets = new ArrayList<>();
        List<List<String>> listNames = new ArrayList<>();
        boolean anyMatchingList = false;
        for (SearchHit hit : searchResponse.getHits()) {
            Map<Integer, SearchHit> chunkByOffset = new LinkedHashMap<>();
            List<String> matchingLists = new ArrayList<>();
            Map<String, SearchHits> inners = hit.getInnerHits() == null ? Map.of() : hit.getInnerHits();
            for (Map.Entry<String, SearchHits> entry : inners.entrySet()) {
                SearchHit[] innerHitArray = entry.getValue().getHits();
                if (innerHitArray.length == 0) {
                    continue;
                }
                SearchHit.NestedIdentity identity = innerHitArray[0].getNestedIdentity();
                if (identity == null || !nestedPath.equals(identity.getField().string())) {
                    continue;
                }
                matchingLists.add(entry.getKey());
                for (SearchHit innerHit : innerHitArray) {
                    chunkByOffset.putIfAbsent(innerHit.getNestedIdentity().getOffset(), innerHit);
                }
            }
            anyMatchingList |= !matchingLists.isEmpty();
            List<Integer> offsets = new ArrayList<>(chunkByOffset.keySet());
            for (Integer offset : offsets) {
                contexts.add(chunkText(chunkByOffset.get(offset), leafField, field));
            }
            chunkOffsets.add(offsets);
            listNames.add(matchingLists);
        }
        boolean innerHitsMissingFromQuery = !anyMatchingList && searchResponse.getHits().getHits().length > 0;
        if (innerHitsMissingFromQuery) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No inner hits on the nested path [%s] found in the search results, so [%s.%s] cannot rerank chunks. "
                            + "Add \"inner_hits\": {} to the nested query on [%s].",
                        nestedPath,
                        NAME,
                        INNER_HITS_FIELD,
                        nestedPath
                    )
                )
            );
            return;
        }
        listener.onResponse(
            new HashMap<>(
                Map.of(
                    DOCUMENT_CONTEXT_LIST_FIELD,
                    contexts,
                    INNER_HITS_CHUNK_OFFSETS_FIELD,
                    chunkOffsets,
                    INNER_HITS_LIST_NAMES_FIELD,
                    listNames
                )
            )
        );
    }

    /** Reads the chunk text from the inner hit's own source, falling back to the full field name (as in semantic highlighting). */
    private String chunkText(final SearchHit innerHit, final String leafField, final String field) {
        Map<String, Object> source = innerHit.hasSource() ? innerHit.getSourceAsMap() : null;
        Object value = source == null ? null : source.get(leafField);
        if (value == null && source != null) {
            value = source.get(field);
        }
        if (value == null) {
            log.warn(
                String.format(
                    Locale.ROOT,
                    "Could not find field %s in nested chunk %d of document %s for reranking! Using the empty string instead.",
                    field,
                    innerHit.getNestedIdentity().getOffset(),
                    innerHit.getId()
                )
            );
            return "";
        }
        // ponytail: a nested leaf is assumed single-valued; a list would stringify as "[a, b]"
        return String.valueOf(value);
    }

    private String contextFromSearchHit(final SearchHit hit, final String field) {
        if (hit.getFields().containsKey(field)) {
            Object fieldValue = hit.field(field).getValue();
            return String.valueOf(fieldValue);
        }
        if (hit.hasSource()) {
            Optional<Object> value = ProcessorUtils.getValueFromSource(hit.getSourceAsMap(), field);
            if (value.isPresent()) {
                Object val = value.get();
                if (val instanceof List) {
                    return ((List<?>) val).stream().map(String::valueOf).collect(Collectors.joining(" "));
                }
                return String.valueOf(val);
            }
        }
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

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create a document context source fetcher from list of field names provided by configuration
     * @param config configuration object grabbed from parsed API request. Should be a list of strings
     * @param innerHits whether to rerank the nested chunks exposed as inner hits instead of the whole document
     * @return a new DocumentContextSourceFetcher or throws IllegalArgumentException if config is malformed
     */
    public static DocumentContextSourceFetcher create(Object config, ClusterService clusterService, boolean innerHits) {
        if (!(config instanceof List)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be a list of field names", NAME));
        }
        List<?> fields = (List<?>) config;
        if (fields.size() == 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be nonempty", NAME));
        }
        if (fields.size() > RERANKER_MAX_DOC_FIELDS.get(clusterService.getSettings())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s must not contain more than %d fields. Configure by setting %s",
                    NAME,
                    RERANKER_MAX_DOC_FIELDS.get(clusterService.getSettings()),
                    RERANKER_MAX_DOC_FIELDS.getKey()
                )
            );
        }
        List<String> fieldsAsStrings = fields.stream().map(field -> (String) field).collect(Collectors.toList());
        boolean singleNestedField = fieldsAsStrings.size() == 1 && fieldsAsStrings.get(0).contains(".");
        if (innerHits && !singleNestedField) {
            throw new IllegalArgumentException(INNER_HITS_REQUIREMENT_MESSAGE);
        }
        return new DocumentContextSourceFetcher(fieldsAsStrings, innerHits);
    }
}
