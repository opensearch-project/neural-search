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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;

public class TextSimilarityRerankProcessor extends RescoringRerankProcessor {

    public static final String MODEL_ID_FIELD = "model_id";

    protected final String modelId;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    public TextSimilarityRerankProcessor(
        String description,
        String tag,
        boolean ignoreFailure,
        String modelId,
        List<ContextSourceFetcher> contextSourceFetchers,
        MLCommonsClientAccessor mlCommonsClientAccessor
    ) {
        super(RerankType.TEXT_SIMILARITY, description, tag, ignoreFailure, contextSourceFetchers);
        this.modelId = modelId;
        this.mlCommonsClientAccessor = mlCommonsClientAccessor;
    }

    @Override
    public void rescoreSearchResponse(SearchResponse response, Map<String, Object> rerankingContext, ActionListener<List<Float>> listener) {
        Object ctxObj = rerankingContext.get(DocumentContextSourceFetcher.DOCUMENT_CONTEXT_LIST_FIELD);
        if (!(ctxObj instanceof List<?>)) {
            listener.onFailure(
                new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "No document context found! Perhaps \"%s.%s\" is missing from the pipeline definition?",
                        RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                        DocumentContextSourceFetcher.NAME
                    )
                )
            );
            return;
        }
        List<?> ctxList = (List<?>) ctxObj;
        List<String> contexts = ctxList.stream().map(str -> (String) str).collect(Collectors.toList());
        mlCommonsClientAccessor.inferenceSimilarity(
            modelId,
            (String) rerankingContext.get(QueryContextSourceFetcher.QUERY_TEXT_FIELD),
            contexts,
            listener
        );
    }

}
