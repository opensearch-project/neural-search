/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank.context;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ObjectPath;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;

/**
 * Context Source Fetcher that gets context from the rerank query ext.
 */
public class QueryContextSourceFetcher implements ContextSourceFetcher {

    public static final String NAME = "query_context";
    public static final String QUERY_TEXT_FIELD = "query_text";
    public static final String QUERY_TEXT_PATH_FIELD = "query_text_path";

    @Override
    public void fetchContext(SearchRequest searchRequest, SearchResponse searchResponse, ActionListener<Map<String, Object>> listener) {
        try {
            List<SearchExtBuilder> exts = searchRequest.source().ext();
            Map<String, Object> params = RerankSearchExtBuilder.fromExtBuilderList(exts).getParams();
            Map<String, Object> scoringContext = new HashMap<>();
            if (!params.containsKey(NAME)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "must specify %s", NAME));
            }
            Object ctxObj = params.remove(NAME);
            if (!(ctxObj instanceof Map<?, ?>)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be a map", NAME));
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> ctxMap = (Map<String, Object>) ctxObj;
            if (ctxMap.containsKey(QUERY_TEXT_FIELD)) {
                if (ctxMap.containsKey(QUERY_TEXT_PATH_FIELD)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Cannot specify both \"%s\" and \"%s\"", QUERY_TEXT_FIELD, QUERY_TEXT_PATH_FIELD)
                    );
                }
                scoringContext.put(QUERY_TEXT_FIELD, (String) ctxMap.get(QUERY_TEXT_FIELD));
            } else if (ctxMap.containsKey(QUERY_TEXT_PATH_FIELD)) {
                String path = (String) ctxMap.get(QUERY_TEXT_PATH_FIELD);
                Map<String, Object> map = requestToMap(searchRequest);
                // Get the text at the path
                Object queryText = ObjectPath.eval(path, map);
                if (!(queryText instanceof String)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "%s must point to a string field", QUERY_TEXT_PATH_FIELD)
                    );
                }
                scoringContext.put(QUERY_TEXT_FIELD, (String) queryText);
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Must specify either \"%s\" or \"%s\"", QUERY_TEXT_FIELD, QUERY_TEXT_PATH_FIELD)
                );
            }
            listener.onResponse(scoringContext);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Convert a search request to a general map by streaming out as XContent and then back in,
     * with the intention of representing the query as a user would see it
     * @param request Search request to turn into xcontent
     * @return Map representing the XContent-ified search request
     * @throws IOException
     */
    private static Map<String, Object> requestToMap(SearchRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentType.CBOR.contentBuilder(baos);
        request.source().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        XContentParser parser = XContentType.CBOR.xContent().createParser(NamedXContentRegistry.EMPTY, null, bais);
        Map<String, Object> map = parser.map();
        return map;
    }
}
