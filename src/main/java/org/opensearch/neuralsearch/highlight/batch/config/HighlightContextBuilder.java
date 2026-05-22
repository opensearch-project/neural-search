/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

/**
 * Expands a {@link HighlightConfig} into per-inference rows of the
 * {@link HighlightContext}. Top-level targets produce one row per matching top
 * hit; nested targets produce one row per inner hit in the target's bucket.
 */
@Log4j2
public class HighlightContextBuilder {

    /**
     * Expands every target in {@code config} against the hits in {@code response}.
     *
     * @param config    resolved per-request config
     * @param response  the search response carrying top hits and inner hits
     * @param startTime epoch millis at which processing started
     * @return aligned-list context ready for batch inference
     */
    public HighlightContext build(HighlightConfig config, SearchResponse response, long startTime) {
        RowAccumulator rows = new RowAccumulator();
        SearchHit[] topHits = response == null || response.getHits() == null ? new SearchHit[0] : response.getHits().getHits();

        if (topHits.length > 0 && config.hasTargets()) {
            for (SemanticHighlightTarget target : config.getTargetsOrEmpty()) {
                String modelId = stringOption(target, SemanticHighlightingConstants.MODEL_ID, null);
                if (modelId == null) {
                    throw new IllegalArgumentException(
                        String.format(
                            java.util.Locale.ROOT,
                            "options.model_id is required on the [%s] semantic highlight field",
                            target.getFieldName()
                        )
                    );
                }
                if (target.isNested()) {
                    expandNestedTarget(target, topHits, config.getQueryText(), modelId, rows);
                } else {
                    expandTopLevelTarget(target, topHits, config.getQueryText(), modelId, rows);
                }
            }
        }

        return HighlightContext.builder()
            .requests(rows.requests)
            .validHits(rows.validHits)
            .fieldNames(rows.fieldNames)
            .preTags(rows.preTags)
            .postTags(rows.postTags)
            .noMatchSizes(rows.noMatchSizes)
            .encoders(rows.encoders)
            .originalResponse(response)
            .startTime(startTime)
            .modelId(resolveModelId(config))
            .maxBatchSize(resolveMaxBatchSize(config))
            .build();
    }

    private void expandTopLevelTarget(
        SemanticHighlightTarget target,
        SearchHit[] topHits,
        String queryText,
        String modelId,
        RowAccumulator rows
    ) {
        for (SearchHit hit : topHits) {
            String text = extractSourceText(hit, target.getFieldName());
            if (text == null || text.isEmpty()) continue;
            rows.add(target, hit, modelId, queryText, text);
        }
    }

    private void expandNestedTarget(
        SemanticHighlightTarget target,
        SearchHit[] topHits,
        String queryText,
        String modelId,
        RowAccumulator rows
    ) {
        String leafField = stripNestedPrefix(target.getFieldName(), target.getNestedPath());
        for (SearchHit topHit : topHits) {
            Map<String, SearchHits> inners = topHit.getInnerHits();
            if (inners == null) continue;
            SearchHits bucket = inners.get(target.getInnerHitsBucketName());
            if (bucket == null || bucket.getHits().length == 0) {
                log.debug(
                    "Inner hits bucket [{}] absent from top hit [{}]; skipping target [{}]",
                    target.getInnerHitsBucketName(),
                    topHit.getId(),
                    target.getFieldName()
                );
                continue;
            }
            for (SearchHit innerHit : bucket.getHits()) {
                String text = extractSourceText(innerHit, leafField);
                if (text == null || text.isEmpty()) continue;
                rows.add(target, innerHit, modelId, queryText, text);
            }
        }
    }

    private static String resolveModelId(HighlightConfig config) {
        for (SemanticHighlightTarget target : config.getTargetsOrEmpty()) {
            String modelId = stringOption(target, SemanticHighlightingConstants.MODEL_ID, null);
            if (modelId != null) return modelId;
        }
        return null;
    }

    private static int resolveMaxBatchSize(HighlightConfig config) {
        for (SemanticHighlightTarget target : config.getTargetsOrEmpty()) {
            Object value = target.getOptions() == null
                ? null
                : target.getOptions().get(SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE);
            if (value instanceof Number) return ((Number) value).intValue();
            if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException ignored) {}
            }
        }
        return SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;
    }

    /**
     * Reads a string value from a hit's {@code _source} under {@code fieldName}.
     * Falls back to the leaf name when the dotted path is absent, because inner
     * hit sources key by leaf, not the fully qualified name.
     */
    private static String extractSourceText(SearchHit hit, String fieldName) {
        if (hit == null) return null;
        Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) return null;
        Object value = source.get(fieldName);
        if (value == null) {
            int dot = fieldName.lastIndexOf('.');
            if (dot >= 0) value = source.get(fieldName.substring(dot + 1));
            if (value == null) return null;
        }
        if (value instanceof String) return (String) value;
        if (value instanceof List) {
            StringBuilder sb = new StringBuilder();
            for (Object v : (List<?>) value) {
                if (v == null) continue;
                if (sb.length() > 0) sb.append(' ');
                sb.append(v);
            }
            return sb.toString();
        }
        return value.toString();
    }

    private static String stripNestedPrefix(String fieldName, String path) {
        if (path == null || path.isEmpty() || fieldName == null) return fieldName;
        String prefix = path + ".";
        return fieldName.startsWith(prefix) ? fieldName.substring(prefix.length()) : fieldName;
    }

    private static String stringOption(SemanticHighlightTarget target, String key, String fallback) {
        if (target.getOptions() == null) return fallback;
        Object v = target.getOptions().get(key);
        if (v instanceof String) {
            String s = (String) v;
            return s.isEmpty() ? fallback : s;
        }
        return fallback;
    }

    private static int intOption(SemanticHighlightTarget target, String key, int fallback) {
        if (target.getOptions() == null) return fallback;
        Object v = target.getOptions().get(key);
        if (v instanceof Number) return ((Number) v).intValue();
        if (v instanceof String) {
            try {
                return Integer.parseInt((String) v);
            } catch (NumberFormatException ignored) {}
        }
        return fallback;
    }

    private static final class RowAccumulator {
        final List<SentenceHighlightingRequest> requests = new ArrayList<>();
        final List<SearchHit> validHits = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        final List<String> preTags = new ArrayList<>();
        final List<String> postTags = new ArrayList<>();
        final List<Integer> noMatchSizes = new ArrayList<>();
        final List<String> encoders = new ArrayList<>();

        void add(SemanticHighlightTarget target, SearchHit targetHit, String modelId, String queryText, String contextText) {
            requests.add(SentenceHighlightingRequest.builder().modelId(modelId).question(queryText).context(contextText).build());
            validHits.add(targetHit);
            fieldNames.add(target.getFieldName());
            preTags.add(target.getPreTag() != null ? target.getPreTag() : SemanticHighlightingConstants.DEFAULT_PRE_TAG);
            postTags.add(target.getPostTag() != null ? target.getPostTag() : SemanticHighlightingConstants.DEFAULT_POST_TAG);
            noMatchSizes.add(
                intOption(target, SemanticHighlightingConstants.NO_MATCH_SIZE, SemanticHighlightingConstants.DEFAULT_NO_MATCH_SIZE)
            );
            encoders.add(stringOption(target, SemanticHighlightingConstants.ENCODER, SemanticHighlightingConstants.DEFAULT_ENCODER));
        }
    }
}
