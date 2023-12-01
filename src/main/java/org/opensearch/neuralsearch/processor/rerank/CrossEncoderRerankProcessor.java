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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ObjectPath;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.ext.RerankSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;

@Log4j2
public class CrossEncoderRerankProcessor extends RescoringRerankProcessor {

    public static final String MODEL_ID_FIELD = "model_id";
    public static final String QUERY_TEXT_FIELD = "query_text";
    public static final String QUERY_TEXT_PATH_FIELD = "query_text_path";
    public static final String RERANK_CONTEXT_FIELD = "rerank_context_field";

    protected final String modelId;
    protected final String rerankContext;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    public CrossEncoderRerankProcessor(
        String description,
        String tag,
        boolean ignoreFailure,
        String modelId,
        String rerankContext,
        MLCommonsClientAccessor mlCommonsClientAccessor
    ) {
        super(RerankType.CROSS_ENCODER, description, tag, ignoreFailure);
        this.modelId = modelId;
        this.rerankContext = rerankContext;
        this.mlCommonsClientAccessor = mlCommonsClientAccessor;
    }

    @Override
    public void generateScoringContext(
        SearchRequest searchRequest,
        SearchResponse searchResponse,
        ActionListener<Map<String, Object>> listener
    ) {
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

    @Override
    public void rescoreSearchResponse(SearchResponse response, Map<String, Object> scoringContext, ActionListener<List<Float>> listener) {
        List<String> contexts = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            contexts.add(contextFromSearchHit(hit));
        }
        mlCommonsClientAccessor.inferenceSimilarity(modelId, (String) scoringContext.get(QUERY_TEXT_FIELD), contexts, listener);
    }

    private String contextFromSearchHit(final SearchHit hit) {
        if (hit.getFields().containsKey(this.rerankContext)) {
            return (String) hit.field(this.rerankContext).getValue();
        } else if (hit.hasSource() && hit.getSourceAsMap().containsKey(this.rerankContext)) {
            return (String) hit.getSourceAsMap().get(this.rerankContext);
        } else {
            return null;
        }
    }

}
