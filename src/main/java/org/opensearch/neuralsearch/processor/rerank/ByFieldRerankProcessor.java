/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A reranking processor that reorders search results based on the content of a specified field.
 * <p>
 * The ByFieldRerankProcessor extends the RescoringRerankProcessor to provide field-based reranking
 * capabilities. It allows for reordering of search results by considering the content of a
 * designated target field within each document.
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
 * </ul>
 * <p>
 * Usage example:
 * <pre>
 * {
 *   "rerank": {
 *     "by_field": {
 *       "target_field": "document.relevance_score",
 *       "remove_target_field": true
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * This processor is particularly useful in scenarios where additional, document-specific
 * information stored in a field can be used to improve the relevance of search results
 * beyond the initial scoring.
 */
public class ByFieldRerankProcessor extends RescoringRerankProcessor {

    public static final String TARGET_FIELD = "target_field";
    public static final String REMOVE_TARGET_FIELD = "remove_target_field";

    protected final String targetField;
    protected final boolean removeTargetField;

    /**
     * Constructor to pass values to the RerankProcessor constructor.
     *
     * @param description           The description of the processor
     * @param tag                   The processor's identifier
     * @param ignoreFailure         If true, OpenSearch ignores any failure of this processor and
     *                              continues to run the remaining processors in the search pipeline.
     *
     * @param targetField           The field you want to replace your <code>_score</code> with
     * @param removeTargetField     A flag to let you delete the target_field for better visualization (i.e. removes a duplicate value)
     * @param contextSourceFetchers  Context from some source and puts it in a map for a reranking processor to use <b> (Unused in ByFieldRerankProcessor)</b>
     */
    public ByFieldRerankProcessor(
        String description,
        String tag,
        boolean ignoreFailure,
        String targetField,
        boolean removeTargetField,
        final List<ContextSourceFetcher> contextSourceFetchers
    ) {
        super(RerankType.BY_FIELD, description, tag, ignoreFailure, contextSourceFetchers);
        this.targetField = targetField;
        this.removeTargetField = removeTargetField;
    }

    @Override
    public void rescoreSearchResponse(SearchResponse response, Map<String, Object> rerankingContext, ActionListener<List<Float>> listener) {
        SearchHit[] searchHits = response.getHits().getHits();

        if (!validateSearchHits(searchHits, listener)) {
            return;
        }

        List<Float> scores = new ArrayList<>(searchHits.length);

        for (SearchHit hit : searchHits) {
            Tuple<? extends MediaType, Map<String, Object>> mediaTypeAndSourceMapTuple = getMediaTypeAndSourceMapTuple(hit);
            Map<String, Object> sourceAsMap = mediaTypeAndSourceMapTuple.v2();

            Object val = getValueFromSource(sourceAsMap, targetField).get();
            scores.add(((Number) val).floatValue());

            sourceAsMap.put("previous_score", hit.getScore());
            if (removeTargetField) {
                removeTargetFieldFromSource(sourceAsMap);
            }

            try {
                XContentBuilder builder = XContentBuilder.builder(mediaTypeAndSourceMapTuple.v1().xContent());
                builder.map(sourceAsMap);
                hit.sourceRef(BytesReference.bytes(builder));
            } catch (IOException e) {
                listener.onFailure(new RuntimeException(e));
                return;
            }
        }

        listener.onResponse(scores);
    }

    /**
     * This helper method is used to initialize the path to take to get to the targetField
     * to remove. It is implemented recursively to delete empty maps as a result of removing the
     * targetField
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #validateSearchHits(SearchHit[], ActionListener)}</b>
     * As such no error cehcking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     */
    private void removeTargetFieldFromSource(Map<String, Object> sourceAsMap) {
        String[] keys = targetField.split("\\.");
        exploreMapAndRemove(sourceAsMap, keys, 0);
    }

    /**
     * This recursive method traces the path to targetField in a sliding window fashion. It does so
     * by passing the parent map and the key (child) to get to the targetField (lastChild). Once it is found it will
     * be deleted. The consequence of this, is having to delete all subsequent empty maps , this is
     * accounted for by the last check to see that the mapping should be removed.
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #validateSearchHits(SearchHit[], ActionListener)}</b>
     * As such no error cehcking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     * @param keys The keys used to traverse the nested map
     * @param currentKeyIndex A sentinel to get the current key to look at
     */
    private void exploreMapAndRemove(Map<String, Object> sourceAsMap, String[] keys, int currentKeyIndex) {
        String child = keys[currentKeyIndex];
        String lastChild = keys[keys.length - 1];

        if (!child.equals(lastChild)) {
            exploreMapAndRemove((Map<String, Object>) sourceAsMap.get(child), keys, currentKeyIndex + 1);
        } else {
            sourceAsMap.remove(child);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> innerMap = (Map<String, Object>) sourceAsMap.get(child);

        if (innerMap != null && innerMap.isEmpty()) {
            sourceAsMap.remove(child);
        }
    }

    /**
     * This is the preflight check for the ByField ReRank Processor. It checks that
     * every Search Hit in the array from a given search Response has all the following
     * for each SearchHit
     * <ul>
     *     <li>Has a <code>_source</code> mapping</li>
     *     <li>Has a valid mapping for <code>target_field</code></li>
     *     <li>That value for the mapping is a valid number</li>
     * </ul>
     * When just one of the conditions fail the exception will be thrown to the listener.
     * @param searchHits from the ByField ReRank Processor
     * @param listener returns an error to the listener in case on of the conditions fail
     * @return The status indicating that the SearchHits are in correct form to perform the Rerank
     */
    private boolean validateSearchHits(SearchHit[] searchHits, ActionListener<List<Float>> listener) {
        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];

            if (!hit.hasSource()) {
                listener.onFailure(
                    new IllegalArgumentException("There is no source field to be able to perform rerank on hit [" + i + "]")
                );
                return false;
            }

            Map<String, Object> sourceMap = getMediaTypeAndSourceMapTuple(hit).v2();
            if (!mappingExistsInSource(sourceMap, targetField)) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + i + "]")
                );
                return false;
            }

            Optional<Object> val = getValueFromSource(sourceMap, targetField);
            if (val.isEmpty()) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is found to be null at hit [" + i + "]")
                );
                return false;
            } else if (!(val.get() instanceof Number)) {
                listener.onFailure(
                    new IllegalArgumentException("The field mapping to rerank [" + targetField + ": " + val.get() + "] is a not Numerical")
                );
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the mapping associated with a path to a value, otherwise
     * returns an empty optional when it encounters a dead end.
     * <hr>
     * When the targetField has the form (key[.key]) it will iterate through
     * the map to see if a mapping exists.
     *
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param pathToValue The path to take to get the desired mapping
     * @return A possible result within an optional
     */
    private Optional<Object> getValueFromSource(Map<String, Object> sourceAsMap, String pathToValue) {
        String[] keys = pathToValue.split("\\.");
        Optional<Object> currentValue = Optional.of(sourceAsMap);

        for (String key : keys) {
            currentValue = currentValue.flatMap(value -> {
                Map<String, Object> currentMap = (Map<String, Object>) value;
                return Optional.of(currentMap.get(key));
            });

            if (currentValue.isEmpty()) {
                return Optional.empty();
            }
        }

        return currentValue;
    }

    /**
     * Determines whether there exists a value that has a mapping according to the pathToValue. This is particularly
     * useful when the source map is a map of maps and when the pathToValue is of the form key[.key]
     * @param sourceAsMap the source field converted to a map
     * @param pathToValue A string of the form key[.key] indicating what keys to apply to the sourceMap
     * @return Whether the mapping using the pathToValue exists
     */
    private boolean mappingExistsInSource(Map<String, Object> sourceAsMap, String pathToValue) {
        return getValueFromSource(sourceAsMap, pathToValue).isPresent();
    }

    /**
     * This helper method is used to retrieve the <code>_source</code> mapping (via v2()) and
     * any metadata associated in this mapping (via v1()).
     *
     * @param hit The searchHit that is expected to have a <code>_source</code> mapping
     * @return Object that contains metadata (MediaType) on the mapping v1() and the actual contents (sourceMap) v2()
     */
    private static Tuple<? extends MediaType, Map<String, Object>> getMediaTypeAndSourceMapTuple(SearchHit hit) {
        BytesReference sourceRef = hit.getSourceRef();
        return XContentHelper.convertToMap(sourceRef, false, (MediaType) null);
    }

}
