/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.opensearch.neuralsearch.highlight.batch.utils.HighlightResultApplier;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HighlightResultApplierTests extends OpenSearchTestCase {

    private HighlightResultApplier applier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        applier = new HighlightResultApplier("<em>", "</em>");
    }

    public void testApplyBatchResults() {
        // Setup
        List<SearchHit> hits = createHits(3);
        List<List<Map<String, Object>>> batchResults = new ArrayList<>();

        // Create results for each hit
        for (int i = 0; i < 3; i++) {
            List<Map<String, Object>> docResults = new ArrayList<>();
            Map<String, Object> highlight = createHighlight(0, 5, 0.9f);
            docResults.add(highlight);
            batchResults.add(docResults);
        }

        // Execute
        applier.applyBatchResults(hits, batchResults, "content", "<mark>", "</mark>");

        // Verify
        for (int i = 0; i < 3; i++) {
            SearchHit hit = hits.get(i);
            assertNotNull(hit.getHighlightFields());
            assertTrue(hit.getHighlightFields().containsKey("content"));

            HighlightField field = hit.getHighlightFields().get("content");
            assertNotNull(field);
            assertEquals(1, field.fragments().length);

            String fragment = field.fragments()[0].toString();
            assertTrue(fragment.contains("<mark>"));
            assertTrue(fragment.contains("</mark>"));
        }
    }

    public void testApplyBatchResultsWithMismatchedSize() {
        // Setup - 3 hits but only 2 results
        List<SearchHit> hits = createHits(3);
        List<List<Map<String, Object>>> batchResults = new ArrayList<>();
        batchResults.add(Collections.singletonList(createHighlight(0, 5, 0.9f)));
        batchResults.add(Collections.singletonList(createHighlight(0, 5, 0.9f)));

        // Execute and expect exception
        assertThrows(IllegalStateException.class, () -> applier.applyBatchResults(hits, batchResults, "content", "<em>", "</em>"));
    }

    public void testApplyBatchResultsWithIndices() {
        // Setup - 5 hits total, applying results to indices 1-3
        List<SearchHit> allHits = createHits(5);
        List<List<Map<String, Object>>> batchResults = new ArrayList<>();

        // Create results for hits at indices 1 and 2 (batch of 2)
        for (int i = 0; i < 2; i++) {
            List<Map<String, Object>> docResults = new ArrayList<>();
            docResults.add(createHighlight(0, 7, 0.95f));
            batchResults.add(docResults);
        }

        // Execute - apply to indices 1-3 (exclusive)
        applier.applyBatchResultsWithIndices(allHits, batchResults, 1, 3, "content", "<b>", "</b>");

        // Verify
        // Hit 0 should have no highlights or empty highlights
        Map<String, HighlightField> hit0Fields = allHits.get(0).getHighlightFields();
        if (hit0Fields != null) {
            assertTrue(hit0Fields.isEmpty());
        }

        // Hits 1 and 2 should have highlights
        for (int i = 1; i < 3; i++) {
            SearchHit hit = allHits.get(i);
            assertNotNull(hit.getHighlightFields());
            assertTrue(hit.getHighlightFields().containsKey("content"));

            HighlightField field = hit.getHighlightFields().get("content");
            String fragment = field.fragments()[0].toString();
            assertTrue(fragment.contains("<b>"));
            assertTrue(fragment.contains("</b>"));
        }

        // Hits 3 and 4 should have no highlights or empty highlights
        Map<String, HighlightField> hit3Fields = allHits.get(3).getHighlightFields();
        if (hit3Fields != null) {
            assertTrue(hit3Fields.isEmpty());
        }

        Map<String, HighlightField> hit4Fields = allHits.get(4).getHighlightFields();
        if (hit4Fields != null) {
            assertTrue(hit4Fields.isEmpty());
        }
    }

    public void testApplySingleResult() {
        // Setup
        SearchHit hit = createHit(0, "This is test content");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        highlights.add(createHighlight(5, 7, 0.9f));  // Highlight "is"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute
        applier.applySingleResult(hit, highlightResults, "content", "<strong>", "</strong>");

        // Verify
        assertNotNull(hit.getHighlightFields());
        HighlightField field = hit.getHighlightFields().get("content");
        assertNotNull(field);

        String fragment = field.fragments()[0].toString();
        assertEquals("This <strong>is</strong> test content", fragment);
    }

    public void testApplySingleResultWithEmptyHighlights() {
        // Setup
        SearchHit hit = createHit(0, "Test content");
        List<Map<String, Object>> emptyResults = new ArrayList<>();

        // Execute
        applier.applySingleResult(hit, emptyResults, "content", "<em>", "</em>");

        // Verify - should have highlight field but with original text
        assertNotNull(hit.getHighlightFields());
        HighlightField field = hit.getHighlightFields().get("content");
        assertNotNull(field);
        assertEquals("Test content", field.fragments()[0].toString());
    }

    public void testApplyMultipleHighlights() {
        // Setup
        SearchHit hit = createHit(0, "The quick brown fox jumps over the lazy dog");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        highlights.add(createHighlight(4, 9, 0.95f));   // "quick"
        highlights.add(createHighlight(16, 19, 0.85f));  // "fox"
        highlights.add(createHighlight(35, 39, 0.75f));  // "lazy"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute
        applier.applySingleResult(hit, highlightResults, "content", "<mark>", "</mark>");

        // Verify
        assertNotNull(hit.getHighlightFields());
        HighlightField field = hit.getHighlightFields().get("content");
        assertNotNull(field);

        String fragment = field.fragments()[0].toString();
        assertEquals("The <mark>quick</mark> brown <mark>fox</mark> jumps over the <mark>lazy</mark> dog", fragment);
    }

    public void testApplyOverlappingHighlights() {
        // Setup
        SearchHit hit = createHit(0, "Test text content");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        // Overlapping highlights - should be handled properly
        highlights.add(createHighlight(0, 4, 0.9f));   // "Test"
        highlights.add(createHighlight(5, 9, 0.8f));   // "text"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute
        applier.applySingleResult(hit, highlightResults, "content", "<em>", "</em>");

        // Verify
        HighlightField field = hit.getHighlightFields().get("content");
        String fragment = field.fragments()[0].toString();
        assertEquals("<em>Test</em> <em>text</em> content", fragment);
    }

    public void testApplyHighlightWithInvalidBounds() {
        // Setup
        SearchHit hit = createHit(0, "Short text");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        // Invalid bounds - should be ignored
        highlights.add(createHighlight(20, 25, 0.9f));  // Beyond text length
        highlights.add(createHighlight(-1, 5, 0.8f));   // Negative start
        highlights.add(createHighlight(5, 3, 0.7f));    // End before start

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute
        applier.applySingleResult(hit, highlightResults, "content", "<em>", "</em>");

        // Verify - original text without highlights
        HighlightField field = hit.getHighlightFields().get("content");
        assertEquals("Short text", field.fragments()[0].toString());
    }

    public void testApplyWithNullFieldValue() {
        // Setup - hit with null field value
        SearchHit hit = new SearchHit(0, "doc0", Collections.emptyMap(), Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        source.put("content", null);
        hit.sourceRef(new BytesArray("{}"));

        List<List<Map<String, Object>>> batchResults = new ArrayList<>();
        batchResults.add(Collections.singletonList(createHighlight(0, 5, 0.9f)));

        // Execute
        applier.applyBatchResults(Collections.singletonList(hit), batchResults, "content", "<em>", "</em>");

        // Verify - should not crash, might have empty highlight fields
        Map<String, HighlightField> fields = hit.getHighlightFields();
        if (fields != null) {
            assertTrue(fields.isEmpty() || !fields.containsKey("content"));
        }
    }

    public void testApplyWithMissingField() {
        // Setup - hit without the target field
        SearchHit hit = new SearchHit(0, "doc0", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"title\":\"Test\"}"));  // Has title but no content

        List<List<Map<String, Object>>> batchResults = new ArrayList<>();
        batchResults.add(Collections.singletonList(createHighlight(0, 5, 0.9f)));

        // Execute
        applier.applyBatchResults(Collections.singletonList(hit), batchResults, "content", "<em>", "</em>");

        // Verify - should not crash, might have empty highlight fields
        Map<String, HighlightField> fields = hit.getHighlightFields();
        if (fields != null) {
            assertTrue(fields.isEmpty() || !fields.containsKey("content"));
        }
    }

    public void testApplyWithCustomTags() {
        // Setup
        SearchHit hit = createHit(0, "Test content");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        highlights.add(createHighlight(0, 4, 0.9f));  // "Test"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute with custom tags
        applier.applySingleResult(hit, highlightResults, "content", "{{", "}}");

        // Verify
        HighlightField field = hit.getHighlightFields().get("content");
        assertEquals("{{Test}} content", field.fragments()[0].toString());
    }

    public void testApplyWithPipelineLevelTags() {
        // Setup - using constructor tags
        HighlightResultApplier customApplier = new HighlightResultApplier("[[", "]]");

        SearchHit hit = createHit(0, "Test content");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        highlights.add(createHighlight(5, 12, 0.9f));  // "content"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute without specifying tags (should use pipeline-level tags)
        customApplier.applySingleResult(hit, highlightResults, "content");

        // Verify
        HighlightField field = hit.getHighlightFields().get("content");
        assertEquals("Test [[content]]", field.fragments()[0].toString());
    }

    public void testSortingHighlightsByPosition() {
        // Setup
        SearchHit hit = createHit(0, "The quick brown fox");

        List<Map<String, Object>> highlightResults = new ArrayList<>();
        Map<String, Object> mlResponse = new HashMap<>();

        List<Map<String, Object>> highlights = new ArrayList<>();
        // Add highlights in random order
        highlights.add(createHighlight(16, 19, 0.7f));  // "fox"
        highlights.add(createHighlight(4, 9, 0.9f));    // "quick"
        highlights.add(createHighlight(10, 15, 0.8f));  // "brown"

        mlResponse.put(SemanticHighlightingConstants.HIGHLIGHTS_KEY, highlights);
        highlightResults.add(mlResponse);

        // Execute
        applier.applySingleResult(hit, highlightResults, "content", "<>", "</>");

        // Verify - should be applied in correct order
        HighlightField field = hit.getHighlightFields().get("content");
        assertEquals("The <>quick</> <>brown</> <>fox</>", field.fragments()[0].toString());
    }

    // Helper methods
    private List<SearchHit> createHits(int count) {
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            hits.add(createHit(i, "Content " + i));
        }
        return hits;
    }

    private SearchHit createHit(int id, String content) {
        SearchHit hit = new SearchHit(id, "doc" + id, Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new BytesArray("{\"content\":\"" + content + "\"}"));
        return hit;
    }

    private Map<String, Object> createHighlight(int start, int end, float score) {
        Map<String, Object> highlight = new HashMap<>();
        highlight.put(SemanticHighlightingConstants.START_KEY, start);
        highlight.put(SemanticHighlightingConstants.END_KEY, end);
        highlight.put("score", score);
        return highlight;
    }
}
