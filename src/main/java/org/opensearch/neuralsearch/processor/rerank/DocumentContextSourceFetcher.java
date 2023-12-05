/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;

/**
 * Context Source Fetcher that gets context from the search results (documents)
 */
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
            return (String) hit.field(field).getValue();
        } else if (hit.hasSource() && hit.getSourceAsMap().containsKey(field)) {
            return (String) hit.getSourceAsMap().get(field);
        } else {
            return "";
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
