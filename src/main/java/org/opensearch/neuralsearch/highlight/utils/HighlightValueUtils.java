/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchException;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;

/**
 * Converts highlightable field values into text elements.
 *
 * <p>Built-in OpenSearch highlighters operate per value: a list field yields one
 * fragment per array element, and scalars are coerced to their string form.
 * Semantic highlighting previously only handled {@code String} on the apply
 * path, silently dropping lists and scalars even though the request path
 * joined them. This utility keeps both paths consistent.</p>
 */
public final class HighlightValueUtils {

    private HighlightValueUtils() {}

    /**
     * Converts a {@code _source} value into highlightable text elements.
     *
     * @param value source value, may be String, Number, Boolean, List, or other
     * @return non-null list of non-empty strings, empty when not highlightable
     */
    public static List<String> toTextElements(final Object value) {
        List<String> elements = new ArrayList<>();
        collectElements(value, elements);
        return elements;
    }

    private static void collectElements(final Object value, final List<String> elements) {
        if (value == null) return;
        if (value instanceof String) {
            String s = (String) value;
            if (!s.isEmpty()) elements.add(s);
            return;
        }
        if (value instanceof Number || value instanceof Boolean) {
            elements.add(value.toString());
            return;
        }
        if (value instanceof List) {
            for (Object item : (List<?>) value) {
                collectElements(item, elements);
            }
            return;
        }
        // Maps and other objects are not highlightable, skip to stay consistent
        // with the single-inference path which only handled strings.
    }

    /**
     * Joins elements the same way the batch request builder does.
     *
     * @param elements non-null text elements
     * @return space-joined string, empty when no elements
     */
    public static String joinElements(final List<String> elements) {
        if (elements == null || elements.isEmpty()) return "";
        return String.join(" ", elements);
    }

    /**
     * Splits joined-string model spans back into per-element highlighted fragments.
     * Each returned entry corresponds to one input element that overlapped at least
     * one span, matching the built-in highlighters which emit one fragment per
     * array element.
     *
     * @param elements text elements in joined order, must be non-null
     * @param highlights model spans with numeric start/end in joined coordinates
     * @param preTag opening tag
     * @param postTag closing tag
     * @return highlighted fragments, empty when no element overlapped
     * @throws OpenSearchException when spans are invalid, unsorted, duplicated, or overlapping
     */
    public static List<String> applyHighlightsPerElement(
        final List<String> elements,
        final List<Map<String, Object>> highlights,
        final String preTag,
        final String postTag
    ) {
        List<String> fragments = new ArrayList<>();
        if (elements == null || elements.isEmpty()) return fragments;
        if (highlights == null || highlights.isEmpty()) return fragments;

        String joined = joinElements(elements);
        List<int[]> spans = HighlightTagApplier.parseAndValidateSpans(highlights, joined.length());

        // Element boundaries in joined coordinates, single space separator.
        List<int[]> boundaries = new ArrayList<>(elements.size());
        int pos = 0;
        for (String element : elements) {
            int start = pos;
            int end = start + element.length();
            boundaries.add(new int[] { start, end });
            pos = end + 1;
        }

        int spanIndex = 0;
        for (int index = 0; index < elements.size(); index++) {
            String element = elements.get(index);
            int elementStart = boundaries.get(index)[0];
            int elementEnd = boundaries.get(index)[1];
            while (spanIndex < spans.size() && spans.get(spanIndex)[1] <= elementStart) {
                spanIndex++;
            }
            List<Map<String, Object>> localHighlights = new ArrayList<>();
            for (int i = spanIndex; i < spans.size() && spans.get(i)[0] < elementEnd; i++) {
                int[] span = spans.get(i);
                int overlapStart = Math.max(span[0], elementStart);
                int overlapEnd = Math.min(span[1], elementEnd);
                if (overlapStart < overlapEnd) {
                    localHighlights.add(
                        Map.of(
                            SemanticHighlightingConstants.START_KEY,
                            overlapStart - elementStart,
                            SemanticHighlightingConstants.END_KEY,
                            overlapEnd - elementStart
                        )
                    );
                }
            }
            if (!localHighlights.isEmpty()) {
                String highlighted = HighlightTagApplier.applyTags(element, localHighlights, preTag, postTag);
                if (highlighted != null) fragments.add(highlighted);
            }
        }
        return fragments;
    }
}
