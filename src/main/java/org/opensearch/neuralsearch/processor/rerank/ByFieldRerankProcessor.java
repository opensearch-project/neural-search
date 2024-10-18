/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.processor.rerank.context.ContextSourceFetcher;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

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
 *       keep_previous_score: false
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

        if (!validateSearchHits(searchHits, listener)) {
            return;
        }

        List<Float> scores = new ArrayList<>(searchHits.length);

        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            float val = getScoreFromSourceMap(sourceAsMap, targetField);
            scores.add(val);

            if (keepPreviousScore) {
                sourceAsMap.put("previous_score", hit.getScore());
            }

            if (removeTargetField) {
                removeTargetFieldFromSource(sourceAsMap);
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

    /**
     * Used to get the numeric mapping from the sourcemap using the <code>target_field</code>
     * <hr>
     * <b>This method assumes that the path to the mapping exists (and is numerical) as checked by {@link #validateSearchHits(SearchHit[], ActionListener)}</b>
     * As such no error checking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     * @param targetField the path to take to get the score to replace by
     * @return The numerical score found using the <code>target_field</code>
     */
    private float getScoreFromSourceMap(Map<String, Object> sourceAsMap, String targetField) {
        Object val = getValueFromSource(sourceAsMap, targetField).get();
        return ((Number) val).floatValue();
    }

    /**
     * This helper method is used to initialize the path to take to get to the targetField
     * to remove as well as the empty maps as the result of the operation.
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #validateSearchHits(SearchHit[], ActionListener)}</b>
     * As such no error checking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     */
    private void removeTargetFieldFromSource(Map<String, Object> sourceAsMap) {
        String[] keys = targetField.split("\\.");
        deleteTargetFieldAndEmptyMaps(sourceAsMap, keys);
    }

    /**
     * This method performs the deletion of the targetField and emptyMaps in 3 phases
     * <ol>
     *     <li>Collect the maps and the respective keys (the key is used to get the inner map) in a stack. It will be used
     *     to delete empty maps and the target field</li>
     *     <li>Delete the top most entry, this is guaranteed even when the source mapping is non nested. This is the
     *     mapping containing the targetField</li>
     *     <li>Iteratively delete the rest of the maps that have (possibly been) emptied as the result of deleting the targetField</li>
     * </ol>
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #validateSearchHits(SearchHit[], ActionListener)}</b>
     * As such no error checking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     * @param keys The keys used to traverse the nested map
     * @implNote You can think of this algorithm as a recursive one the base case is deleting the targetField. The recursive case
     * is going to the next map along with the respective key. Along the way if it finds a map is empty it will delete it
     */
    private void deleteTargetFieldAndEmptyMaps(Map<String, Object> sourceAsMap, String[] keys) {
        Stack<Tuple<Map<String, Object>, String>> parentMapChildrenKeyTupleStack = new Stack<>();

        Map<String, Object> currentMap = sourceAsMap;
        String lastKey = keys[keys.length - 1];

        // Collect the parent maps with respective children to use them inside out
        for (String key : keys) {
            parentMapChildrenKeyTupleStack.add(new Tuple<>(currentMap, key));
            if (key.equals(lastKey)) {
                break;
            }
            currentMap = (Map<String, Object>) currentMap.get(key);
        }

        // Remove the last key this is guaranteed
        Tuple<Map<String, Object>, String> currentParentMapWithChild = parentMapChildrenKeyTupleStack.pop();
        Map<String, Object> parentMap = currentParentMapWithChild.v1();
        String key = currentParentMapWithChild.v2();
        parentMap.remove(key);

        // Delete the empty maps inside out using the stack to mock a recursive solution
        while (!parentMapChildrenKeyTupleStack.isEmpty()) {
            currentParentMapWithChild = parentMapChildrenKeyTupleStack.pop();
            parentMap = currentParentMapWithChild.v1();
            key = currentParentMapWithChild.v2();

            @SuppressWarnings("unchecked")
            Map<String, Object> innerMap = (Map<String, Object>) parentMap.get(key);

            if (innerMap != null && innerMap.isEmpty()) {
                parentMap.remove(key);
            }
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

            Map<String, Object> sourceMap = hit.getSourceAsMap();
            if (!mappingExistsInSource(sourceMap, targetField)) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + i + "]")
                );
                return false;
            }

            Optional<Object> val = getValueFromSource(sourceMap, targetField);

            if (!(val.get() instanceof Number)) {
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
                return Optional.ofNullable(currentMap.get(key));
            });

            if (currentValue.isEmpty()) {
                return Optional.empty();
            }
        }

        return currentValue;
    }

    /**
     * Determines whether there exists a value that has a mapping according to the pathToValue. This is particularly
     * useful when the source map is a map of maps and when the pathToValue is of the form key[.key].
     * <hr>
     * To Exist in a map it must have a mapping that is not null or the key-value pair does not exist
     * @param sourceAsMap the source field converted to a map
     * @param pathToValue A string of the form key[.key] indicating what keys to apply to the sourceMap
     * @return Whether the mapping using the pathToValue exists
     */
    private boolean mappingExistsInSource(Map<String, Object> sourceAsMap, String pathToValue) {
        return getValueFromSource(sourceAsMap, pathToValue).isPresent();
    }

}
