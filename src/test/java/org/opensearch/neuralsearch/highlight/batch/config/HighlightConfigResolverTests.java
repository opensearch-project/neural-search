/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.script.Script;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class HighlightConfigResolverTests extends OpenSearchTestCase {

    public void testEmptyWhenRequestIsNull() {
        HighlightConfig config = HighlightConfigResolver.resolve(null);
        assertFalse(config.hasTargets());
        assertNull(config.getQueryText());
    }

    public void testEmptyWhenRequestHasNoSemanticHighlight() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field("body");
        f.highlighterType("unified");
        hl.field(f);
        src.highlighter(hl);
        src.query(new MatchQueryBuilder("body", "x"));
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        assertFalse(config.hasTargets());
    }

    public void testTopLevelSemanticTargetCaptured() {
        SearchRequest request = buildTopLevelRequest("summary", "what are treatments");
        HighlightConfig config = HighlightConfigResolver.resolve(request);
        assertTrue(config.hasTargets());
        assertEquals(1, config.getTargetsOrEmpty().size());
        SemanticHighlightTarget target = config.getTargetsOrEmpty().get(0);
        assertEquals("summary", target.getFieldName());
        assertFalse(target.isNested());
        assertEquals("what are treatments", config.getQueryText());
    }

    public void testMultipleTopLevelSemanticFieldsAllCaptured() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        for (String name : new String[] { "summary", "abstract", "body" }) {
            HighlightBuilder.Field f = new HighlightBuilder.Field(name);
            f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
            hl.field(f);
        }
        src.highlighter(hl);
        src.query(new MatchQueryBuilder("body", "treatments"));
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        assertEquals(3, config.getTargetsOrEmpty().size());
    }

    public void testInnerHitsOnlyCustomerBugShape() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();

        HighlightBuilder innerHl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field("chunks.text");
        f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        innerHl.field(f);

        InnerHitBuilder inner = new InnerHitBuilder();
        inner.setHighlightBuilder(innerHl);

        NestedQueryBuilder nested = new NestedQueryBuilder("chunks", new MatchQueryBuilder("chunks.text", "treatments"), ScoreMode.Avg)
            .innerHit(inner);
        src.query(nested);
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        assertEquals(1, config.getTargetsOrEmpty().size());
        SemanticHighlightTarget target = config.getTargetsOrEmpty().get(0);
        assertEquals("chunks.text", target.getFieldName());
        assertEquals("chunks", target.getNestedPath());
        assertEquals("chunks", target.getInnerHitsBucketName());
        assertTrue(target.isNested());
    }

    public void testInnerHitsExplicitBucketName() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();

        HighlightBuilder innerHl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field("sections.body");
        f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        innerHl.field(f);

        InnerHitBuilder inner = new InnerHitBuilder().setName("matching_sections");
        inner.setHighlightBuilder(innerHl);

        NestedQueryBuilder nested = new NestedQueryBuilder("sections", new MatchQueryBuilder("sections.body", "treatments"), ScoreMode.Avg)
            .innerHit(inner);
        src.query(nested);
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        SemanticHighlightTarget target = config.getTargetsOrEmpty().get(0);
        assertEquals("matching_sections", target.getInnerHitsBucketName());
    }

    public void testResolvesInnerHitsInsideBoolMust() {
        BoolQueryBuilder bool = new BoolQueryBuilder().must(buildNestedWithInnerHit("chunks"));
        HighlightConfig config = resolveWithQuery(bool);
        assertEquals(1, config.getTargetsOrEmpty().size());
    }

    public void testResolvesInnerHitsInsideDisMax() {
        DisMaxQueryBuilder dm = new DisMaxQueryBuilder().add(buildNestedWithInnerHit("chunks"));
        HighlightConfig config = resolveWithQuery(dm);
        assertEquals(1, config.getTargetsOrEmpty().size());
    }

    public void testResolvesInnerHitsInsideConstantScore() {
        ConstantScoreQueryBuilder cs = new ConstantScoreQueryBuilder(buildNestedWithInnerHit("chunks"));
        HighlightConfig config = resolveWithQuery(cs);
        assertEquals(1, config.getTargetsOrEmpty().size());
    }

    public void testResolvesInnerHitsInsideBoosting() {
        BoostingQueryBuilder b = new BoostingQueryBuilder(buildNestedWithInnerHit("chunks"), buildNestedWithInnerHit("notes"));
        HighlightConfig config = resolveWithQuery(b);
        assertEquals(2, config.getTargetsOrEmpty().size());
    }

    public void testResolvesInnerHitsInsideFunctionScore() {
        FunctionScoreQueryBuilder fs = new FunctionScoreQueryBuilder(buildNestedWithInnerHit("chunks"));
        HighlightConfig config = resolveWithQuery(fs);
        assertEquals(1, config.getTargetsOrEmpty().size());
    }

    public void testResolvesInnerHitsInsideScriptScore() {
        ScriptScoreQueryBuilder ss = new ScriptScoreQueryBuilder(buildNestedWithInnerHit("chunks"), new Script("1"));
        HighlightConfig config = resolveWithQuery(ss);
        assertEquals(1, config.getTargetsOrEmpty().size());
    }

    public void testMixedTopLevelAndInnerHitsCaptureBoth() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field summary = new HighlightBuilder.Field("summary");
        summary.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        hl.field(summary);
        src.highlighter(hl);
        src.query(buildNestedWithInnerHit("chunks"));
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        List<SemanticHighlightTarget> targets = config.getTargetsOrEmpty();
        assertEquals(2, targets.size());
        // top-level comes first
        assertEquals("summary", targets.get(0).getFieldName());
        assertFalse(targets.get(0).isNested());
        assertTrue(targets.get(1).isNested());
    }

    public void testNonSemanticFieldsAreIgnored() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field semantic = new HighlightBuilder.Field("body");
        semantic.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        HighlightBuilder.Field unified = new HighlightBuilder.Field("title");
        unified.highlighterType("unified");
        hl.field(semantic);
        hl.field(unified);
        src.highlighter(hl);
        src.query(new MatchQueryBuilder("body", "treatments"));
        request.source(src);

        HighlightConfig config = HighlightConfigResolver.resolve(request);
        assertEquals(1, config.getTargetsOrEmpty().size());
        assertEquals("body", config.getTargetsOrEmpty().get(0).getFieldName());
    }

    private static SearchRequest buildTopLevelRequest(String fieldName, String queryText) {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field(fieldName);
        f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        hl.field(f);
        src.highlighter(hl);
        src.query(new MatchQueryBuilder(fieldName, queryText));
        request.source(src);
        return request;
    }

    private static NestedQueryBuilder buildNestedWithInnerHit(String path) {
        HighlightBuilder innerHl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field(path + ".text");
        f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        innerHl.field(f);

        InnerHitBuilder inner = new InnerHitBuilder();
        inner.setHighlightBuilder(innerHl);

        return new NestedQueryBuilder(path, new MatchQueryBuilder(path + ".text", "x"), ScoreMode.Avg).innerHit(inner);
    }

    private static HighlightConfig resolveWithQuery(org.opensearch.index.query.QueryBuilder query) {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder src = new SearchSourceBuilder();
        src.query(query);
        request.source(src);
        return HighlightConfigResolver.resolve(request);
    }
}
