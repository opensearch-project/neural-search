/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.OpenSearchException;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;

/**
 * Applies highlight tags to text based on position spans returned by a model.
 * Shared by both single-inference and batch-inference highlighting paths.
 */
public class HighlightTagApplier {

    /**
     * Applies highlight tags to text based on position spans from model output.
     * Expects spans to be sorted by start position with no overlaps or duplicate start positions.
     * All spans must have numeric start/end values within text bounds.
     *
     * @param text The original text to highlight
     * @param highlights List of maps with "start" and "end" keys representing highlight positions
     * @param preTag The opening tag for highlighted text
     * @param postTag The closing tag for highlighted text
     * @return The text with highlight tags applied, or null if highlights is null or empty
     * @throws OpenSearchException if any span is invalid (non-numeric, out-of-bounds, unsorted, duplicate start, or overlapping)
     */
    public static String applyTags(String text, List<Map<String, Object>> highlights, String preTag, String postTag) {
        if (highlights == null || highlights.isEmpty()) {
            return null;
        }

        List<int[]> spans = new ArrayList<>(highlights.size());
        for (Map<String, Object> highlight : highlights) {
            Object startObj = highlight.get(SemanticHighlightingConstants.START_KEY);
            Object endObj = highlight.get(SemanticHighlightingConstants.END_KEY);
            if (!ProcessorUtils.isNumeric(startObj) || !ProcessorUtils.isNumeric(endObj)) {
                throw new OpenSearchException(
                    String.format(
                        Locale.ROOT,
                        "Invalid highlight positions: start and end must be numeric, but got start=%s, end=%s",
                        startObj,
                        endObj
                    )
                );
            }
            int start = ((Number) startObj).intValue();
            int end = ((Number) endObj).intValue();
            if (start < 0 || end > text.length() || start >= end) {
                throw new OpenSearchException(
                    String.format(
                        Locale.ROOT,
                        "Invalid highlight positions: start=%d, end=%d, textLength=%d. Positions must satisfy: 0 <= start < end <= textLength",
                        start,
                        end,
                        text.length()
                    )
                );
            }
            spans.add(new int[] { start, end });
        }

        for (int i = 1; i < spans.size(); i++) {
            int prevStart = spans.get(i - 1)[0];
            int prevEnd = spans.get(i - 1)[1];
            int currStart = spans.get(i)[0];
            if (currStart < prevStart) {
                throw new OpenSearchException("Invalid highlight positions: highlights are not sorted by start position");
            }
            if (currStart == prevStart) {
                throw new OpenSearchException(
                    String.format(Locale.ROOT, "Invalid highlight positions: duplicate start position %d", currStart)
                );
            }
            if (currStart < prevEnd) {
                throw new OpenSearchException(
                    String.format(
                        Locale.ROOT,
                        "Invalid highlight positions: overlapping spans [%d,%d) and [%d,%d)",
                        prevStart,
                        prevEnd,
                        currStart,
                        spans.get(i)[1]
                    )
                );
            }
        }

        int resultSize = text.length() + (preTag.length() + postTag.length()) * spans.size();
        StringBuilder result = new StringBuilder(resultSize);
        int currentPos = 0;
        for (int[] span : spans) {
            if (span[0] > currentPos) {
                result.append(text, currentPos, span[0]);
            }
            result.append(preTag);
            result.append(text, span[0], span[1]);
            result.append(postTag);
            currentPos = span[1];
        }
        if (currentPos < text.length()) {
            result.append(text, currentPos, text.length());
        }
        return result.toString();
    }
}
