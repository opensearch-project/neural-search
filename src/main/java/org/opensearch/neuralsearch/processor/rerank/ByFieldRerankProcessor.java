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

public class ByFieldRerankProcessor extends RescoringRerankProcessor {

    public static final String TARGET_FIELD = "target_field";

    protected final String targetField;

    /**
     * Constructor. pass through to RerankProcessor constructor.
     *
     * @param description
     * @param tag
     * @param ignoreFailure
     * @param targetField           the field you want to replace your score with
     * @param contextSourceFetchers
     */
    public ByFieldRerankProcessor(
        String description,
        String tag,
        boolean ignoreFailure,
        String targetField,
        final List<ContextSourceFetcher> contextSourceFetchers
    ) {
        super(RerankType.BY_FIELD, description, tag, ignoreFailure, contextSourceFetchers);
        this.targetField = targetField;
    }

    @Override
    public void rescoreSearchResponse(SearchResponse response, Map<String, Object> rerankingContext, ActionListener<List<Float>> listener) {
        SearchHit[] searchHits = response.getHits().getHits();
        searchHitsHaveValidForm(searchHits, listener);
        List<Float> scores = new ArrayList<>(searchHits.length);

        for (SearchHit hit : searchHits) {
            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = getMapTuple(hit);
            Map<String, Object> sourceAsMap = getMapTuple(hit).v2();

            XContentBuilder builder = null;
            try {
                builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                sourceAsMap.put("previous_score", hit.getScore());
                builder.map(sourceAsMap);
                hit.sourceRef(BytesReference.bytes(builder));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Object val = sourceAsMap.get(targetField);
            scores.add(((Number) val).floatValue());
        }
        listener.onResponse(scores);
    }

    private void searchHitsHaveValidForm(SearchHit[] searchHits, ActionListener<List<Float>> listener) {
        for (int i = 0; i < searchHits.length; i++) {
            SearchHit hit = searchHits[i];

            if (!hit.hasSource()) {
                listener.onFailure(
                    new IllegalArgumentException("There is no source field to be able to perform rerank on hit [" + i + "]")
                );
            }

            Map<String, Object> sourceMap = getMapTuple(hit).v2();
            if (!sourceMap.containsKey(targetField)) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is not found at hit [" + i + "]")
                );
            }

            Object val = sourceMap.get(targetField);
            if (val == null) {
                listener.onFailure(
                    new IllegalArgumentException("The field to rerank [" + targetField + "] is found to be null at hit [" + i + "]")
                );
            } else if (!(val instanceof Number)) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "The field mapping to rerank [" + targetField + ": " + sourceMap.get(targetField) + "] is a not of type Number"
                    )
                );
            }
        }
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
