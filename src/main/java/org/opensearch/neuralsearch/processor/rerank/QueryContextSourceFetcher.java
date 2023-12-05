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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
            if (params.containsKey(QUERY_TEXT_FIELD)) {
                if (params.containsKey(QUERY_TEXT_PATH_FIELD)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Cannot specify both \"%s\" and \"%s\"", QUERY_TEXT_FIELD, QUERY_TEXT_PATH_FIELD)
                    );
                }
                scoringContext.put(QUERY_TEXT_FIELD, (String) params.get(QUERY_TEXT_FIELD));
            } else if (params.containsKey(QUERY_TEXT_PATH_FIELD)) {
                String path = (String) params.get(QUERY_TEXT_PATH_FIELD);
                // Convert query to a map with io/xcontent shenanigans
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                XContentBuilder builder = XContentType.CBOR.contentBuilder(baos);
                searchRequest.source().toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                XContentParser parser = XContentType.CBOR.xContent().createParser(NamedXContentRegistry.EMPTY, null, bais);
                Map<String, Object> map = parser.map();
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

    public String getName() {
        return NAME;
    }
}
