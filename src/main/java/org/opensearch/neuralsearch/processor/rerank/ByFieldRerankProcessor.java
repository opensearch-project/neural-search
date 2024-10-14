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

public class ByFieldRerankProcessor extends RescoringRerankProcessor {

    public static final String TARGET_FIELD = "target_field";
    public static final String REMOVE_TARGET_FIELD = "remove_target_field";

    protected final String targetField;
    protected final boolean removeTargetField;

    /**
     * Constructor. pass through to RerankProcessor constructor.
     *
     * @param description
     * @param tag
     * @param ignoreFailure
     * @param targetField           the field you want to replace your score with
     * @param removeTargetField
     * @param contextSourceFetchers
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

        if (!searchHitsHaveValidForm(searchHits, listener)) {
            return;
        }

        List<Float> scores = new ArrayList<>(searchHits.length);

        for (SearchHit hit : searchHits) {
            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = getMapTuple(hit);
            Map<String, Object> sourceAsMap = typeAndSourceMap.v2();

            Object val = getValueFromMap(sourceAsMap, targetField).get();
            scores.add(((Number) val).floatValue());

            sourceAsMap.put("previous_score", hit.getScore());
            if (removeTargetField) {
                removeTargetFieldFromMap(sourceAsMap);
            }

            try {
                XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                builder.map(sourceAsMap);
                hit.sourceRef(BytesReference.bytes(builder));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        listener.onResponse(scores);
    }

    /**
     * This helper method is used to initialize the path to take to get to the targetField
     * to remove. It is implemented recursively to delete empty maps as a result of removing the
     * targetField
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #searchHitsHaveValidForm(SearchHit[], ActionListener)}</b>
     * As such no error cehcking is done in the methods implementing this functionality
     * @param sourceAsMap the map of maps that contains the <code>targetField</code>
     */
    private void removeTargetFieldFromMap(Map<String, Object> sourceAsMap) {
        String[] keys = targetField.split("\\.");
        exploreMapAndRemove(sourceAsMap, keys, 0);
    }

    /**
     * This recursive method traces the path to targetField in a sliding window fashion. It does so
     * by passing the parent map and the key (child) to get to the targetField (lastChild). Once it is found it will
     * be deleted. The consequence of this, is having to delete all subsequent empty maps , this is
     * accounted for by the last check to see that the mapping should be removed.
     * <hr>
     * <b>This method assumes that the path to the mapping exists as checked by {@link #searchHitsHaveValidForm(SearchHit[], ActionListener)}</b>
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

    private boolean searchHitsHaveValidForm(SearchHit[] searchHits, ActionListener<List<Float>> listener) {
        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];

            if (!hit.hasSource()) {
                listener.onFailure(
                    new IllegalArgumentException("There is no source field to be able to perform rerank on hit [" + i + "]")
                );
                return false;
            }

            Map<String, Object> sourceMap = getMapTuple(hit).v2();
            if (!containsMapping(sourceMap, targetField)) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + i + "]")
                );
                return false;
            }

            Optional<Object> val = getValueFromMap(sourceMap, targetField);
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
     * @param map         the map you want to iterate through
     * @param pathToValue the path to take to get the desired mapping
     * @return A possible result within an optional
     */
    private Optional<Object> getValueFromMap(Map<String, Object> map, String pathToValue) {
        String[] keys = pathToValue.split("\\.");
        Optional<Object> currentValue = Optional.of(map);

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

    private boolean containsMapping(Map<String, Object> map, String pathToValue) {
        return getValueFromMap(map, pathToValue).isPresent();
    }

    /**
     * This helper method is used to retrieve the <code>_source</code> mapping (via v2()) and
     * any metadata associated in this mapping (via v2()).
     *
     * @param hit The searchHit that is expected to have a <code>_source</code> mapping
     * @return Object that contains metadata on the mapping v1() and the actual contents v2()
     */
    private static Tuple<? extends MediaType, Map<String, Object>> getMapTuple(SearchHit hit) {
        BytesReference sourceRef = hit.getSourceRef();
        return XContentHelper.convertToMap(sourceRef, false, (MediaType) null);
    }

}
