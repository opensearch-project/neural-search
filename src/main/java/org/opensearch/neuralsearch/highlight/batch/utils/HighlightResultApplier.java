/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.core.common.text.Text;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.utils.HighlightEncoders;
import org.opensearch.neuralsearch.highlight.utils.HighlightTagApplier;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;

/**
 * Applies model output to {@link SearchHit#getHighlightFields()}. Each row has its
 * own field name, tag pair, encoder and no_match_size, supplied through aligned
 * parallel lists from {@link org.opensearch.neuralsearch.highlight.batch.HighlightContext}.
 */
@Log4j2
public class HighlightResultApplier {

    /** Apply a full batch — one row per (target, hit) pair. */
    public void applyBatchResults(
        List<SearchHit> validHits,
        List<List<Map<String, Object>>> batchResults,
        List<String> fieldNames,
        List<String> preTags,
        List<String> postTags,
        List<Integer> noMatchSizes,
        List<String> encoders
    ) {
        if (batchResults.size() != validHits.size()) {
            log.error("Batch results size ({}) does not match valid hits size ({})", batchResults.size(), validHits.size());
            throw new IllegalStateException("Batch results size mismatch");
        }
        for (int i = 0; i < validHits.size(); i++) {
            applyRow(
                validHits.get(i),
                batchResults.get(i),
                fieldNames.get(i),
                preTags.get(i),
                postTags.get(i),
                noMatchSizes.get(i),
                encoders.get(i)
            );
        }
    }

    /** Apply a slice [startIndex, endIndex) of a paginated execution. */
    public void applyBatchResultsWithIndices(
        List<SearchHit> allValidHits,
        List<List<Map<String, Object>>> sliceResults,
        int startIndex,
        int endIndex,
        List<String> fieldNames,
        List<String> preTags,
        List<String> postTags,
        List<Integer> noMatchSizes,
        List<String> encoders
    ) {
        int expected = endIndex - startIndex;
        if (sliceResults.size() != expected) {
            log.error(
                "Slice results size ({}) does not match expected size ({}) for [{}, {})",
                sliceResults.size(),
                expected,
                startIndex,
                endIndex
            );
            throw new IllegalStateException("Batch results size mismatch");
        }
        int sliceIdx = 0;
        for (int i = startIndex; i < endIndex && sliceIdx < sliceResults.size(); i++, sliceIdx++) {
            applyRow(
                allValidHits.get(i),
                sliceResults.get(sliceIdx),
                fieldNames.get(i),
                preTags.get(i),
                postTags.get(i),
                noMatchSizes.get(i),
                encoders.get(i)
            );
        }
    }

    private void applyRow(
        SearchHit hit,
        List<Map<String, Object>> highlights,
        String fieldName,
        String preTag,
        String postTag,
        int noMatchSize,
        String encoder
    ) {
        Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) return;

        // Inner hit _source is keyed by leaf name (e.g. "text"), not the fully qualified
        // dotted name (e.g. "chunks.text"). Fall back to the leaf when the dotted path
        // is absent.
        String text = readString(source, fieldName);
        if (text == null) {
            int dot = fieldName.lastIndexOf('.');
            if (dot >= 0) text = readString(source, fieldName.substring(dot + 1));
        }
        if (text == null || text.isEmpty()) return;

        String highlighted = HighlightTagApplier.applyTags(text, highlights, preTag, postTag);
        if (highlighted == null) {
            if (noMatchSize > 0) {
                String snippet = text.length() <= noMatchSize ? text : text.substring(0, noMatchSize);
                if (SemanticHighlightingConstants.ENCODER_HTML.equalsIgnoreCase(encoder)) {
                    snippet = HighlightEncoders.htmlEscape(snippet);
                }
                writeHighlightField(hit, fieldName, snippet);
            }
            return;
        }

        if (SemanticHighlightingConstants.ENCODER_HTML.equalsIgnoreCase(encoder)) {
            highlighted = HighlightEncoders.htmlEncodePreservingTags(highlighted, preTag, postTag);
        }
        writeHighlightField(hit, fieldName, highlighted);
    }

    private static String readString(Map<String, Object> source, String key) {
        Object v = source.get(key);
        if (v instanceof String) return (String) v;
        return null;
    }

    private static void writeHighlightField(SearchHit hit, String fieldName, String value) {
        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
        if (highlightFields == null) {
            highlightFields = new HashMap<>();
        } else if (!(highlightFields instanceof HashMap)) {
            highlightFields = new HashMap<>(highlightFields);
        }
        highlightFields.put(fieldName, new HighlightField(fieldName, new Text[] { new Text(value) }));
        hit.highlightFields(highlightFields);
    }
}
