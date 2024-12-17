/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor.InferenceRequest;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.QueryContextSourceFetcher;

/**
 * Rescoring Rerank Processor that uses a TextSimilarity model in ml-commons to rescore
 */
public class MLOpenSearchRerankProcessor extends RescoringRerankProcessor {

    public static final String MODEL_ID_FIELD = "model_id";

    protected final String modelId;

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;

    /**
     * Constructor
     * @param description
     * @param tag
     * @param ignoreFailure
     * @param modelId id of TEXT_SIMILARITY model
     * @param contextSourceFetchers
     * @param mlCommonsClientAccessor
     */
    public MLOpenSearchRerankProcessor(
        final String description,
        final String tag,
        final boolean ignoreFailure,
        final String modelId,
        final List<ContextSourceFetcher> contextSourceFetchers,
        final MLCommonsClientAccessor mlCommonsClientAccessor
    ) {
        super(RerankType.ML_OPENSEARCH, description, tag, ignoreFailure, contextSourceFetchers);
        this.modelId = modelId;
        this.mlCommonsClientAccessor = mlCommonsClientAccessor;
    }

    @Override
    public void rescoreSearchResponse(
        final SearchResponse response,
        final Map<String, Object> rerankingContext,
        final ActionListener<List<Float>> listener
    ) {
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
            InferenceRequest.builder()
                .modelId(modelId)
                .queryText((String) rerankingContext.get(QueryContextSourceFetcher.QUERY_TEXT_FIELD))
                .inputTexts(contexts)
                .build(),
            listener
        );
    }

}
