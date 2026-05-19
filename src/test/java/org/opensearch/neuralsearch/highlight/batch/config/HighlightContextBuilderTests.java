/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.batch.HighlightContext;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

public class HighlightContextBuilderTests extends OpenSearchTestCase {

    public void testTopLevelTargetEnqueuesOneRequestPerHit() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .preTag("<em>")
            .postTag("</em>")
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchHit[] hits = new SearchHit[] { hitWithSource("1", Map.of("body", "alpha")), hitWithSource("2", Map.of("body", "beta")) };
        SearchResponse response = mockResponse(hits);

        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(2, ctx.size());
        assertEquals(2, ctx.getValidHits().size());
        assertEquals("body", ctx.getFieldNames().get(0));
        assertEquals("m1", ctx.getModelId());
    }

    public void testTargetWithoutModelIdIsSkipped() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget noModel = SemanticHighlightTarget.builder().fieldName("body").options(Map.of()).build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(noModel)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "alpha")) });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(0, ctx.size());
    }

    public void testHitWithMissingFieldIsSkipped() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("title", "no body field")) });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(0, ctx.size());
    }

    public void testEmptyContextWhenNoHits() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[0]);
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(0, ctx.size());
    }

    public void testMultipleTopLevelTargetsProduceOneRowPerFieldPerHit() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget summary = SemanticHighlightTarget.builder()
            .fieldName("summary")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        SemanticHighlightTarget abstractField = SemanticHighlightTarget.builder()
            .fieldName("abstract")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(summary, abstractField)).queryText("treatments").build();

        SearchHit[] hits = new SearchHit[] {
            hitWithSource("1", Map.of("summary", "s1", "abstract", "a1")),
            hitWithSource("2", Map.of("summary", "s2", "abstract", "a2")) };
        SearchResponse response = mockResponse(hits);

        HighlightContext ctx = builder.build(config, response, 0L);
        // 2 targets x 2 hits = 4 rows
        assertEquals(4, ctx.size());
    }

    public void testNestedTargetEnqueuesOneRequestPerInnerHit() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("chunks.text")
            .nestedPath("chunks")
            .innerHitsBucketName("chunks")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .preTag("<em>")
            .postTag("</em>")
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        // Create a top hit with inner hits
        SearchHit innerHit1 = hitWithSource("1", Map.of("text", "inner text 1"));
        SearchHit innerHit2 = hitWithSource("1", Map.of("text", "inner text 2"));
        SearchHits innerHits = new SearchHits(
            new SearchHit[] { innerHit1, innerHit2 },
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        SearchHit topHit = hitWithSource("1", Map.of("title", "paper"));
        topHit.setInnerHits(Map.of("chunks", innerHits));

        SearchResponse response = mockResponse(new SearchHit[] { topHit });
        HighlightContext ctx = builder.build(config, response, 0L);

        assertEquals(2, ctx.size());
        assertEquals("chunks.text", ctx.getFieldNames().get(0));
        assertEquals("chunks.text", ctx.getFieldNames().get(1));
    }

    public void testNestedTargetWithCustomBucketName() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("sections.body")
            .nestedPath("sections")
            .innerHitsBucketName("matching_sections")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchHit innerHit = hitWithSource("1", Map.of("body", "section text"));
        SearchHits innerHits = new SearchHits(new SearchHit[] { innerHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);

        SearchHit topHit = hitWithSource("1", Map.of("title", "paper"));
        topHit.setInnerHits(Map.of("matching_sections", innerHits));

        SearchResponse response = mockResponse(new SearchHit[] { topHit });
        HighlightContext ctx = builder.build(config, response, 0L);

        assertEquals(1, ctx.size());
    }

    public void testNestedTargetSkipsWhenBucketMissing() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("chunks.text")
            .nestedPath("chunks")
            .innerHitsBucketName("chunks")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        // Top hit with no inner hits at all
        SearchHit topHit = hitWithSource("1", Map.of("title", "paper"));
        SearchResponse response = mockResponse(new SearchHit[] { topHit });
        HighlightContext ctx = builder.build(config, response, 0L);

        assertEquals(0, ctx.size());
    }

    public void testNullResponseProducesEmptyContext() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        HighlightContext ctx = builder.build(config, null, 0L);
        assertEquals(0, ctx.size());
    }

    public void testMixedTopLevelAndNestedTargets() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget topLevel = SemanticHighlightTarget.builder()
            .fieldName("summary")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        SemanticHighlightTarget nested = SemanticHighlightTarget.builder()
            .fieldName("chunks.text")
            .nestedPath("chunks")
            .innerHitsBucketName("chunks")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(topLevel, nested)).queryText("treatments").build();

        SearchHit innerHit = hitWithSource("1", Map.of("text", "inner text"));
        SearchHits innerHits = new SearchHits(new SearchHit[] { innerHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);

        SearchHit topHit = hitWithSource("1", Map.of("summary", "top level text"));
        topHit.setInnerHits(Map.of("chunks", innerHits));

        SearchResponse response = mockResponse(new SearchHit[] { topHit });
        HighlightContext ctx = builder.build(config, response, 0L);

        // 1 top-level row + 1 nested row = 2
        assertEquals(2, ctx.size());
        assertEquals("summary", ctx.getFieldNames().get(0));
        assertEquals("chunks.text", ctx.getFieldNames().get(1));
    }

    private static SearchHit hitWithSource(String id, Map<String, Object> source) {
        SearchHit hit = new SearchHit(0, id, new HashMap<>(), null);
        BytesReference json = new BytesArray(toJson(source));
        hit.sourceRef(json);
        return hit;
    }

    private static String toJson(Map<String, Object> source) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(entry.getKey()).append("\":\"").append(entry.getValue()).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private static SearchResponse mockResponse(SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 1, null, SearchResponse.Clusters.EMPTY);
    }

    public void testMaxBatchSizeFromOptions() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1", SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE, 50))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(50, ctx.getMaxBatchSize());
    }

    public void testMaxBatchSizeFromStringOption() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1", SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE, "25"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(25, ctx.getMaxBatchSize());
    }

    public void testDefaultMaxBatchSizeWhenNotSpecified() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE, ctx.getMaxBatchSize());
    }

    public void testNestedTargetWithEmptyInnerHitsText() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("chunks.text")
            .nestedPath("chunks")
            .innerHitsBucketName("chunks")
            .options(Map.of(SemanticHighlightingConstants.MODEL_ID, "m1"))
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchHit innerHit = hitWithSource("1", Map.of("text", ""));
        SearchHits innerHits = new SearchHits(new SearchHit[] { innerHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);

        SearchHit topHit = hitWithSource("1", Map.of("title", "paper"));
        topHit.setInnerHits(Map.of("chunks", innerHits));

        SearchResponse response = mockResponse(new SearchHit[] { topHit });
        HighlightContext ctx = builder.build(config, response, 0L);
        assertEquals(0, ctx.size());
    }

    public void testNoMatchSizeAndEncoderFromOptions() {
        HighlightContextBuilder builder = new HighlightContextBuilder();
        SemanticHighlightTarget target = SemanticHighlightTarget.builder()
            .fieldName("body")
            .options(
                Map.of(
                    SemanticHighlightingConstants.MODEL_ID,
                    "m1",
                    SemanticHighlightingConstants.NO_MATCH_SIZE,
                    10,
                    SemanticHighlightingConstants.ENCODER,
                    "html"
                )
            )
            .preTag("<b>")
            .postTag("</b>")
            .build();
        HighlightConfig config = HighlightConfig.builder().targets(List.of(target)).queryText("treatments").build();

        SearchResponse response = mockResponse(new SearchHit[] { hitWithSource("1", Map.of("body", "text")) });
        HighlightContext ctx = builder.build(config, response, 0L);

        assertEquals(1, ctx.size());
        assertEquals(Integer.valueOf(10), ctx.getNoMatchSizes().get(0));
        assertEquals("html", ctx.getEncoders().get(0));
        assertEquals("<b>", ctx.getPreTags().get(0));
        assertEquals("</b>", ctx.getPostTags().get(0));
    }
}
