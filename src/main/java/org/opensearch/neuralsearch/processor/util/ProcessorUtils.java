/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

/**
 * Utility class for evaluating SearchResponse data. This is useful when you want
 * to see that the searchResponse is in correct form or if the data you want to extract/edit
 * from the SearchResponse
 */
public class ProcessorUtils {

    /**
     * Represents a function used to validate a <code>SearchHit</code> based on the provided implementation
     * When it is incorrect an Exception is expected to be thrown. Otherwise, no return value is given
     * to the caller indicating that the <code>SearchHit</code> is valid.
     *<hr>
     * <p>This is a <a href="package-summary.html">functional interface</a>
     * whose functional method is {@link #validate(SearchHit)}}.
     */
    @FunctionalInterface
    public interface SearchHitValidator {

        /**
         * Performs the validation for the SearchHit and takes in metadata of what happened when the error occurred.
         * <hr>
         * When the SearchHit is not in correct form, an exception is thrown
         * @param hit The specific SearchHit were the invalidation occurred
         * @throws IllegalArgumentException if the validation for the hit fails
         */
        void validate(final SearchHit hit) throws IllegalArgumentException;
    }

    /**
     * This is the preflight check for Reranking. It checks that
     * every Search Hit in the array from a given search Response has all the following
     * for each SearchHit follows the correct form as specified by the validator.
     * When just one of the conditions fail (as specified by the validator) the exception will be thrown to the listener.
     * @param searchHits from the SearchResponse
     * @param listener returns an error to the listener in case on of the conditions fail
     * @return The status indicating that the SearchHits are in correct form to perform the Rerank
     */
    public static boolean validateRerankCriteria(
        final SearchHit[] searchHits,
        final SearchHitValidator validator,
        final ActionListener<List<Float>> listener
    ) {
        for (SearchHit hit : searchHits) {
            try {
                validator.validate(hit);
            } catch (IllegalArgumentException e) {
                listener.onFailure(e);
                return false;
            }
        }
        return true;
    }

    /**
     * Used to get the numeric mapping from the sourcemap using the <code>target_field</code>
     * <hr>
     * <b>This method assumes that the path to the mapping exists (and is numerical) as checked by {@link #validateRerankCriteria(SearchHit[], SearchHitValidator, ActionListener)}</b>
     * As such no error checking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     * @param targetField the path to take to get the score to replace by
     * @return The numerical score found using the <code>target_field</code>
     */
    public static float getScoreFromSourceMap(final Map<String, Object> sourceAsMap, final String targetField) {
        Object val = getValueFromSource(sourceAsMap, targetField).get();
        return ((Number) val).floatValue();
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
     * <b>This method assumes that the path to the mapping exists as checked by {@link #validateRerankCriteria(SearchHit[], SearchHitValidator, ActionListener)}</b>
     * As such no error checking is done in the methods implementing this functionality
     * <hr>
     *  You can think of this algorithm as a recursive one the base case is deleting the targetField. The recursive case
     *  is going to the next map along with the respective key. Along the way if it finds a map is empty it will delete it
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     * @param targetField The path to take to remove the targetField
     */
    public static void removeTargetFieldFromSource(final Map<String, Object> sourceAsMap, final String targetField) {
        Stack<Tuple<Map<String, Object>, String>> parentMapChildrenKeyTupleStack = new Stack<>();
        String[] keys = targetField.split("\\.");

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
     * Returns the mapping associated with a path to a value, otherwise
     * returns an empty optional when it encounters a dead end.
     * <hr>
     * When the targetField has the form (key[.key]) it will iterate through
     * the map to see if a mapping exists.
     *
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param targetField The path to take to get the desired mapping
     * @return A possible result within an optional
     */
    public static Optional<Object> getValueFromSource(final Map<String, Object> sourceAsMap, final String targetField) {
        String[] keys = targetField.split("\\.");
        Optional<Object> currentValue = Optional.of(sourceAsMap);

        for (String key : keys) {
            currentValue = currentValue.flatMap(value -> {
                if (!(value instanceof Map<?, ?>)) {
                    return Optional.empty();
                }
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
    public static boolean mappingExistsInSource(final Map<String, Object> sourceAsMap, final String pathToValue) {
        return getValueFromSource(sourceAsMap, pathToValue).isPresent();
    }

}
