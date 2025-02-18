/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;

import java.util.HashMap;
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
     * @param validator The given validator used to check every search hit being correct
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
        if (val instanceof String) {
            return Float.parseFloat((String) val);
        }
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
        return getValueFromSource(sourceAsMap, targetField, -1);
    }

    public static Optional<Object> getValueFromSource(final Map<String, Object> sourceAsMap, final String targetField, int index) {
        String[] keys = targetField.split("\\.");
        Optional<Object> currentValue = Optional.ofNullable(sourceAsMap);

        for (String key : keys) {
            currentValue = currentValue.flatMap(value -> {
                if (value instanceof List && index != -1) {
                    Object listValue = ((List) value).get(index);
                    if (listValue instanceof Map) {
                        Map<String, Object> currentMap = (Map<String, Object>) listValue;
                        return Optional.ofNullable(currentMap.get(key));
                    }
                } else if (!(value instanceof Map<?, ?>)) {
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
     * Given the path to targetKey in sourceAsMap, sets targetValue in targetKey
     *
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param targetKey The path to key to insert the desired targetValue
     * @param targetValue The value to insert to targetKey
     */
    public static void setValueToSource(Map<String, Object> sourceAsMap, String targetKey, Object targetValue) {
        setValueToSource(sourceAsMap, targetKey, targetValue, -1);
    }

    public static void setValueToSource(Map<String, Object> sourceAsMap, String targetKey, Object targetValue, int index) {
        if (sourceAsMap == null || targetKey == null) return;

        String[] keys = targetKey.split("\\.");
        Map<String, Object> current = sourceAsMap;

        for (int i = 0; i < keys.length - 1; i++) {
            Object next = current.computeIfAbsent(keys[i], k -> new HashMap<>());
            if (next instanceof List<?> list) {
                if (index < 0 || index >= list.size()) return;
                current = (Map<String, Object>) list.get(index);
            } else if (next instanceof Map<?, ?>) {
                current = (Map<String, Object>) next;
            } else {
                throw new IllegalStateException("Unexpected data structure at " + keys[i]);
            }
        }

        String lastKey = keys[keys.length - 1];
        Object existingValue = current.get(lastKey);

        if (existingValue instanceof List<?> existingList) {
            if (index >= 0 && index < existingList.size()) {
                ((List<Object>) existingList).set(index, targetValue);
            } else if (index == -1) {
                ((List<Object>) existingList).add(targetValue);
            }
        } else {
            current.put(lastKey, targetValue);
        }
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

    /**
     * @param value Any value to be determined to be numerical
     * @return whether the value can be turned into a number
     */
    public static boolean isNumeric(Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Number) {
            return true;
        }

        if (value instanceof String) {
            String string = (String) value;
            try {
                Double.parseDouble(string);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Given path, new key, and level, return a new path with given new key
     * e.g:
     * path: level1.level2.oldKey
     * textKey: newKey
     * level: 2
     * returns level1.level2.newKey
     *
     * @param path path to old key
     * @param textKey new key to replace in old key
     * @param level level of the traversal
     * @return path with new key
     */
    public static String computeFullTextKey(String path, String textKey, int level) {
        String[] keys = path.split("\\.", level);
        keys[keys.length - 1] = textKey;
        return String.join(".", keys);
    }

    /**
     * Given a map, path to value, and level in the map, return the key mapped with given value.
     * if there are multiple keys mapping with same value, return the last key
     * e.g:
     *
     * map:
     *  {
     *     "level1": {
     *          "level2" : {
     *              "first_text": "passage_embedding",
     *              "second_text": "passage_embedding"
     *          }
     *      }
     * }
     * path: "level1.level2.passage_embedding"
     * level: 3
     * returns "second_text".
     *
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param path The path to key to insert the desired mapping
     */
    public static String findKeyFromFromValue(Map<String, Object> sourceAsMap, String path, int level) {
        String[] keys = path.split("\\.", level);
        Map<String, Object> currentMap = sourceAsMap;
        String targetValue = keys[keys.length - 1];
        for (String key : keys) {
            if (key.equals(targetValue)) {
                break;
            }
            if (currentMap.containsKey(key)) {
                Object value = currentMap.get(key);
                if (value instanceof Map) {
                    currentMap = (Map<String, Object>) value;
                }
            }
        }
        String lastFoundKey = null;
        for (Map.Entry<String, Object> entry : currentMap.entrySet()) {
            if (entry.getValue().equals(targetValue)) {
                lastFoundKey = entry.getKey();
            }
        }
        return lastFoundKey;
    }
}
