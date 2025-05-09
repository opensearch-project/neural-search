/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import lombok.NonNull;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.search.SearchHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            currentMap = unsafeCastToObjectMap(currentMap.get(key));
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

            Map<String, Object> innerMap = unsafeCastToObjectMap(parentMap.get(key));

            if (innerMap != null && innerMap.isEmpty()) {
                parentMap.remove(key);
            }
        }
    }

    /**
     * Returns the list of all mappings associated with a path to a value, otherwise
     * returns an empty optional when it encounters a dead end.
     * When the targetField has the form (key[.key]) it will iterate through
     * the map to see if a mapping exists. If result is an instance of List, a nested List is returned
     *
     * single value e.g:
     * given
     * map:
     *    level1: {level2: value}
     * targetField:
     *    level1.level2
     * returns value
     *
     * multiple value e.g:
     * given
     * map:
     *    level1: [{level2: value1}, {level2:value2}]
     * targetField:
     *    level1.level2
     * returns [value1, value2]
     *
     * list value e.g:
     * given
     * map:
     *    level1: level2: [value1, value2]
     * targetField:
     *    level1.level2
     * returns [[value1, value2]]
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param targetField The path to take to get the desired mapping
     * @return A possible result within an optional
     */
    public static Optional<Object> getValueFromSource(final Map<String, Object> sourceAsMap, final String targetField) {
        String[] keys = targetField.split("\\.");
        Object current = sourceAsMap;

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (current instanceof Map) {
                current = unsafeCastToObjectMap(current).get(key);
                // If it's the last key and the value is a List<String>, wrap it inside another list
                if (i == keys.length - 1 && current instanceof List) {
                    if (unsafeCastToObjectList(current).isEmpty() == false
                        && unsafeCastToObjectList(current).getFirst() instanceof String) {
                        return Optional.of(List.of(current));
                    }
                }
            } else if (current instanceof List) {
                List<Object> results = parseList(unsafeCastToObjectList(current), key);
                if (results.isEmpty()) {
                    return Optional.empty();
                }
                current = results;
            } else {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(current);
    }

    // iterate each Map Object in given list and return a list of map values with given key
    private static List<Object> parseList(List<Object> list, String key) {
        List<Object> result = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map) {
                Object value = unsafeCastToObjectMap(item).get(key);
                if (value != null) {
                    result.add(value);
                }
            }
        }
        return result;
    }

    public static void setValueToSource(Map<String, Object> sourceAsMap, String targetKey, Object targetValue) {
        setValueToSource(sourceAsMap, targetKey, targetValue, -1);
    }

    /**
     * Inserts or updates a value in a nested map structure, with optional support for list traversal.
     * This method navigates through the provided sourceAsMap using the dot-delimited key path
     * specified by targetKey. Intermediate maps are created as needed. When a List is encountered,
     * the provided index is used to select the element from the list. The selected element must be a map to
     * continue the traversal.
     * Once the final map in the path is reached, the method sets the value for the last key.
     *
     * @param sourceAsMap The Source map (a map of maps) to iterate through
     * @param targetKey   he path to key to insert the desired targetValue
     * @param targetValue the value to set at the specified key path
     * @param index       the index to use when a list is encountered during traversal; if list processing is not needed,
     *                    -1 is passed in
     */

    public static void setValueToSource(Map<String, Object> sourceAsMap, String targetKey, Object targetValue, int index) {
        if (Objects.isNull(sourceAsMap) || Objects.isNull(targetKey)) return;

        String[] keys = targetKey.split("\\.");
        Map<String, Object> current = sourceAsMap;

        for (int i = 0; i < keys.length - 1; i++) {
            Object next = current.computeIfAbsent(keys[i], k -> new HashMap<>());
            if (next instanceof ArrayList<?> list) {
                if (index < 0 || index >= list.size()) return;
                if (list.get(index) instanceof Map) {
                    current = unsafeCastToObjectMap(list.get(index));
                }
            } else if (next instanceof Map) {
                current = unsafeCastToObjectMap(next);
            } else {
                throw new IllegalStateException("Unexpected data structure at " + keys[i]);
            }
        }
        String lastKey = keys[keys.length - 1];
        current.put(lastKey, targetValue);
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
     * Returns the number of subqueries that are present in the queryTopDocs. This is useful to determine
     * if the queryTopDocs are empty or not
     * @param queryTopDocs
     * @return
     */
    public static int getNumOfSubqueries(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> !topDocs.getTopDocs().isEmpty())
            .findAny()
            .get()
            .getTopDocs()
            .size();
    }

    // This method should be used only when you are certain the object is a `Map<String, Object>`.
    // It is recommended to use this method as a last resort.
    @SuppressWarnings("unchecked")
    public static Map<String, Object> unsafeCastToObjectMap(Object obj) {
        return (Map<String, Object>) obj;
    }

    // This method should be used only when you are certain the object is a `List<Object>`.
    // It is recommended to use this method as a last resort.
    @SuppressWarnings("unchecked")
    public static List<Object> unsafeCastToObjectList(Object obj) {
        return (List<Object>) obj;
    }

    public static int getMaxTokenCount(
        @NonNull final Map<String, Object> sourceAndMetadataMap,
        @NonNull final Settings settings,
        @NonNull final ClusterService clusterService
    ) {
        int defaultMaxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings);
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (Objects.isNull(indexMetadata)) {
            return defaultMaxTokenCount;
        }
        // if the index is specified in the metadata, read maxTokenCount from the index setting
        return IndexSettings.MAX_TOKEN_COUNT_SETTING.get(indexMetadata.getSettings());
    }
}
