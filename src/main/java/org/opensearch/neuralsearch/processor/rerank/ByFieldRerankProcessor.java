/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.SearchHitValidator;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.getScoreFromSourceMap;
import static org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.getValueFromSource;
import static org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.mappingExistsInSource;
import static org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.removeTargetFieldFromSource;
import static org.opensearch.neuralsearch.processor.util.processorSearchResponseUtil.validateRerankCriteria;

/**
 * A reranking processor that reorders search results based on the content of a specified field.
 * <p>
 * The ByFieldRerankProcessor allows for reordering of search results by considering the content of a
 * designated target field within each document. This processor will update the <code>_score</code> field with what has been provided
 * by {@code target_field}. When {@code keep_previous_score} is enabled a new field is appended called <code>previous_score</code> which was the score prior to reranking.
 * <p>
 * Key features:
 * <ul>
 *   <li>Reranks search results based on a specified target field</li>
 *   <li>Optionally removes the target field from the final search results</li>
 *   <li>Supports nested field structures using dot notation</li>
 * </ul>
 * <p>
 * The processor uses the following configuration parameters:
 * <ul>
 *   <li>{@code target_field}: The field to be used for reranking (required)</li>
 *   <li>{@code remove_target_field}: Whether to remove the target field from the final results (optional, default: false)</li>
 *   <li>{@code keep_previous_score}: Whether to append the previous score in a field called <code>previous_score</code> (optional, default: false)</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>
 * {
 *   "rerank": {
 *     "by_field": {
 *       "target_field": "document.relevance_score",
 *       "remove_target_field": true,
 *       "keep_previous_score": false
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * This processor is useful in scenarios where additional, document-specific
 * information stored in a field can be used to improve the relevance of search results
 * beyond the initial scoring.
 */
public class ByFieldRerankProcessor extends RescoringRerankProcessor {

    public static final String TARGET_FIELD = "target_field";
    public static final String REMOVE_TARGET_FIELD = "remove_target_field";
    public static final String KEEP_PREVIOUS_SCORE = "keep_previous_score";

    protected final String targetField;
    protected final boolean removeTargetField;
    protected final boolean keepPreviousScore;

    /**
     * Constructor to pass values to the RerankProcessor constructor.
     *
     * @param description           The description of the processor
     * @param tag                   The processor's identifier
     * @param ignoreFailure         If true, OpenSearch ignores any failure of this processor and
     *                              continues to run the remaining processors in the search pipeline.
     * @param targetField           The field you want to replace your <code>_score</code> with
     * @param removeTargetField     A flag to let you delete the target_field for better visualization (i.e. removes a duplicate value)
     * @param keepPreviousScore     A flag to let you decide to stash your previous <code>_score</code> in a field called <code>previous_score</code> (i.e. for debugging purposes)
     * @param contextSourceFetchers  Context from some source and puts it in a map for a reranking processor to use <b> (Unused in ByFieldRerankProcessor)</b>
     */
    public ByFieldRerankProcessor(
        String description,
        String tag,
        boolean ignoreFailure,
        String targetField,
        boolean removeTargetField,
        boolean keepPreviousScore,
        final List<ContextSourceFetcher> contextSourceFetchers
    ) {
        super(RerankType.BY_FIELD, description, tag, ignoreFailure, contextSourceFetchers);
        this.targetField = targetField;
        this.removeTargetField = removeTargetField;
        this.keepPreviousScore = keepPreviousScore;
    }

    @Override
    public void rescoreSearchResponse(SearchResponse response, Map<String, Object> rerankingContext, ActionListener<List<Float>> listener) {
        SearchHit[] searchHits = response.getHits().getHits();

        SearchHitValidator searchHitValidator = this::byFieldSearchHitValidator;

        if (!validateRerankCriteria(searchHits, searchHitValidator, listener)) {
            return;
        }

        List<Float> scores = new ArrayList<>(searchHits.length);

        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            float score = getScoreFromSourceMap(sourceAsMap, targetField);
            scores.add(score);

            if (keepPreviousScore) {
                sourceAsMap.put("previous_score", hit.getScore());
            }

            if (removeTargetField) {
                String[] keysToTargetField = targetField.split("\\.");
                removeTargetFieldFromSource(sourceAsMap, keysToTargetField);
            }

            try {
                XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
                BytesReference sourceMapAsBytes = BytesReference.bytes(builder.map(sourceAsMap));
                hit.sourceRef(sourceMapAsBytes);
            } catch (IOException e) {
                listener.onFailure(new RuntimeException(e));
                return;
            }
        }

        listener.onResponse(scores);
    }

    public void byFieldSearchHitValidator(SearchHit hit) {
        if (!hit.hasSource()) {
            throw new IllegalArgumentException("There is no source field to be able to perform rerank on hit [" + hit.docId() + "]");
        }

        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (!mappingExistsInSource(sourceMap, targetField)) {
            throw new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + hit.docId() + "]");
        }

        Optional<Object> val = getValueFromSource(sourceMap, targetField);

        if (!(val.get() instanceof Number)) {
            throw new IllegalArgumentException("The field mapping to rerank [" + targetField + ": " + val.get() + "] is a not Numerical");
        }

    }
}
