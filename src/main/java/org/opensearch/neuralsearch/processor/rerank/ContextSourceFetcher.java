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

import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;

/**
 * Interface that gets context from some source and puts it in a map
 * for a reranking processor to use
 */
public interface ContextSourceFetcher {

    /**
     * Fetch the information needed in order to rerank.
     * That could be as simple as grabbing a field from the search request or
     * as complicated as a lookup to some external service
     * @param searchRequest the search query
     * @param searchResponse the search results, in case they're relevant
     * @param listener be async
     */
    public void fetchContext(SearchRequest searchRequest, SearchResponse searchResponse, ActionListener<Map<String, Object>> listener);

    /**
     * Get the name of the contextSourceFetcher. This will be used as the field
     * name in the context config for the pipeline
     * @return Name of the fetcher
     */
    public String getName();

}
