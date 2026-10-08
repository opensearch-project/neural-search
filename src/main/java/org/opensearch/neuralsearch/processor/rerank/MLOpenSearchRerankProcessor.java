/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SimilarityInferenceRequest;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.context.QueryContextSourceFetcher;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

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
        EventStatsManager.increment(EventStatName.RERANK_ML_PROCESSOR_EXECUTIONS);
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
        ActionListener<List<Float>> scoreListener = listener;
        if (rerankingContext.containsKey(DocumentContextSourceFetcher.INNER_HITS_CHUNK_OFFSETS_FIELD)) {
            if (contexts.isEmpty()) {
                listener.onResponse(Collections.nCopies(response.getHits().getHits().length, Float.NaN));
                return;
            }
            scoreListener = innerHitsScoreListener(response, rerankingContext, listener);
        }
        mlCommonsClientAccessor.inferenceSimilarity(
            SimilarityInferenceRequest.builder()
                .modelId(modelId)
                .queryText((String) rerankingContext.get(QueryContextSourceFetcher.QUERY_TEXT_FIELD))
                .inputTexts(contexts)
                .build(),
            scoreListener
        );
    }

    /**
     * Spreads the flat chunk scores back over each hit's inner hits lists, re-sorts them and reports the per-hit maximum.
     */
    @SuppressWarnings("unchecked")
    private ActionListener<List<Float>> innerHitsScoreListener(
        final SearchResponse response,
        final Map<String, Object> rerankingContext,
        final ActionListener<List<Float>> listener
    ) {
        List<List<Integer>> chunkOffsets = (List<List<Integer>>) rerankingContext.get(
            DocumentContextSourceFetcher.INNER_HITS_CHUNK_OFFSETS_FIELD
        );
        List<List<String>> listNames = (List<List<String>>) rerankingContext.get(DocumentContextSourceFetcher.INNER_HITS_LIST_NAMES_FIELD);
        return ActionListener.wrap(scores -> {
            SearchHit[] hits = response.getHits().getHits();
            List<Float> hitScores = new ArrayList<>(hits.length);
            int cursor = 0;
            for (int i = 0; i < hits.length; i++) {
                Map<Integer, Float> scoreByOffset = new HashMap<>();
                float maxScore = Float.NaN;
                for (Integer offset : chunkOffsets.get(i)) {
                    float score = scores.get(cursor++);
                    scoreByOffset.put(offset, score);
                    if (Float.isNaN(maxScore) || score > maxScore) {
                        maxScore = score;
                    }
                }
                hitScores.add(maxScore);
                List<String> names = listNames.get(i);
                if (names.isEmpty()) {
                    continue;
                }
                Map<String, SearchHits> innerHits = new HashMap<>(hits[i].getInnerHits());
                for (String name : names) {
                    SearchHits list = innerHits.get(name);
                    SearchHit[] chunks = list.getHits();
                    for (SearchHit chunk : chunks) {
                        chunk.score(scoreByOffset.get(chunk.getNestedIdentity().getOffset()));
                    }
                    Arrays.sort(chunks, (chunk1, chunk2) -> Float.compare(chunk2.getScore(), chunk1.getScore()));
                    innerHits.put(
                        name,
                        new SearchHits(
                            chunks,
                            list.getTotalHits(),
                            chunks[0].getScore(),
                            list.getSortFields(),
                            list.getCollapseField(),
                            list.getCollapseValues()
                        )
                    );
                }
                hits[i].setInnerHits(innerHits);
            }
            if (cursor != scores.size()) {
                throw new IllegalStateException("scores and nested chunks are not the same length");
            }
            listener.onResponse(hitScores);
        }, listener::onFailure);
    }

}
