/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class HighlightValueUtilsTests extends OpenSearchTestCase {

    public void testToTextElementsWithString() {
        assertEquals(List.of("alpha"), HighlightValueUtils.toTextElements("alpha"));
    }

    public void testToTextElementsSkipsEmptyString() {
        assertTrue(HighlightValueUtils.toTextElements("").isEmpty());
    }

    public void testToTextElementsWithScalars() {
        assertEquals(List.of("42"), HighlightValueUtils.toTextElements(42));
        assertEquals(List.of("true"), HighlightValueUtils.toTextElements(true));
    }

    public void testToTextElementsWithListSkipsNullsAndObjects() {
        java.util.ArrayList<Object> values = new java.util.ArrayList<>();
        values.add("alpha");
        values.add(null);
        values.add(42);
        values.add(Map.of("key", "value"));
        values.add("gamma");
        assertEquals(List.of("alpha", "42", "gamma"), HighlightValueUtils.toTextElements(values));
    }

    public void testToTextElementsFlattensNestedLists() {
        assertEquals(List.of("a", "b", "c"), HighlightValueUtils.toTextElements(List.of("a", List.of("b", "c"))));
    }

    public void testApplyHighlightsPerElementSplitsAcrossElements() {
        List<String> elements = List.of("alpha", "beta", "gamma");
        // Joined is "alpha beta gamma", beta is [6,10)
        List<String> fragments = HighlightValueUtils.applyHighlightsPerElement(
            elements,
            List.of(Map.of("start", 6, "end", 10)),
            "<em>",
            "</em>"
        );
        assertEquals(1, fragments.size());
        assertEquals("<em>beta</em>", fragments.get(0));
    }

    public void testApplyHighlightsPerElementReturnsOneFragmentPerMatchingElement() {
        List<String> elements = List.of("alpha", "beta");
        // Joined "alpha beta": alpha [0,5), beta [6,10)
        List<String> fragments = HighlightValueUtils.applyHighlightsPerElement(
            elements,
            List.of(Map.of("start", 0, "end", 5), Map.of("start", 6, "end", 10)),
            "<em>",
            "</em>"
        );
        assertEquals(List.of("<em>alpha</em>", "<em>beta</em>"), fragments);
    }

    public void testApplyHighlightsPerElementWithScalar() {
        List<String> fragments = HighlightValueUtils.applyHighlightsPerElement(
            List.of("42"),
            List.of(Map.of("start", 0, "end", 2)),
            "<em>",
            "</em>"
        );
        assertEquals(List.of("<em>42</em>"), fragments);
    }
}
