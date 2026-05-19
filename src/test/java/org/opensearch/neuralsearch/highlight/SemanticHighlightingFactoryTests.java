/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.neuralsearch.highlight.batch.processor.SemanticHighlightingFactory;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.ext.SemanticHighlighterExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class SemanticHighlightingFactoryTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    @Mock
    private Processor.PipelineContext pipelineContext;

    private SemanticHighlightingFactory factory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        factory = new SemanticHighlightingFactory(mlClientAccessor);
    }

    /** Top-level type: semantic field alone is enough to trigger the factory (legacy path). */
    public void testShouldGenerateReturnsTrueForTopLevelSemanticField() {
        SearchRequest request = buildRequestWithTopLevelSemanticField();
        assertTrue(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    /** Non-semantic highlighter type does not trigger the factory. */
    public void testShouldGenerateReturnsFalseForNonSemanticHighlighting() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType("plain");
        hl.field(field);
        source.highlighter(hl);
        request.source(source);

        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    /** Customer bug shape: only inner_hits has type: semantic, no top-level highlight, no ext.
     *  The cheap candidate check returns false and the query tree is NOT walked. */
    public void testShouldGenerateReturnsFalseForInnerHitsOnlyWithoutExt() {
        SearchRequest request = buildRequestWithInnerHitsSemanticField();
        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    /** Customer bug shape + ext opt-in: factory walks the tree and finds the inner_hits target. */
    public void testShouldGenerateReturnsTrueForInnerHitsSemanticFieldWithExt() {
        SearchRequest request = buildRequestWithInnerHitsSemanticField();
        request.source().ext(extEnabled(true));
        assertTrue(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    /** ext: false explicitly disables the ext signal. Top-level semantic field still triggers. */
    public void testShouldGenerateRespectsExtFalseButHonorsTopLevel() {
        SearchRequest request = buildRequestWithTopLevelSemanticField();
        request.source().ext(extEnabled(false));
        assertTrue(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    /** ext: false alone (no top-level semantic, no inner_hits) → factory short-circuits. */
    public void testShouldGenerateReturnsFalseWithExtFalseAndNoSemanticField() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.ext(extEnabled(false));
        request.source(source);

        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    public void testShouldGenerateReturnsFalseWhenNoHighlighter() {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    public void testShouldGenerateReturnsFalseWhenNoSearchSource() {
        SearchRequest request = new SearchRequest();
        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    public void testShouldGenerateReturnsFalseWhenNullRequest() {
        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(null)));
    }

    public void testShouldGenerateWithMultipleFieldsOneIsSemantic() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();

        HighlightBuilder.Field plain = new HighlightBuilder.Field("title");
        plain.highlighterType("plain");
        hl.field(plain);

        HighlightBuilder.Field semantic = new HighlightBuilder.Field("content");
        semantic.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        hl.field(semantic);

        source.highlighter(hl);
        request.source(source);

        assertTrue(factory.shouldGenerate(new ProcessorGenerationContext(request)));
    }

    public void testCreateProcessorWithDefaultValues() {
        Map<String, Object> config = new HashMap<>();
        SearchResponseProcessor processor = factory.create(new HashMap<>(), null, null, false, config, pipelineContext);

        assertNotNull(processor);
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
        assertFalse(processor.isIgnoreFailure());
        assertEquals(SemanticHighlightingConstants.PROCESSOR_TYPE, processor.getType());
    }

    public void testCreateProcessorWithIgnoreFailure() {
        Map<String, Object> config = new HashMap<>();
        SearchResponseProcessor processor = factory.create(new HashMap<>(), "tag", "desc", true, config, pipelineContext);

        assertNotNull(processor);
        // Factory always supplies the canonical tag/description.
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
        assertTrue(processor.isIgnoreFailure());
    }

    public void testFactoryType() {
        assertEquals("semantic-highlighter", SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE);
    }

    private static SearchRequest buildRequestWithTopLevelSemanticField() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        HighlightBuilder hl = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        hl.field(field);
        source.highlighter(hl);
        request.source(source);
        return request;
    }

    private static SearchRequest buildRequestWithInnerHitsSemanticField() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();

        HighlightBuilder innerHl = new HighlightBuilder();
        HighlightBuilder.Field f = new HighlightBuilder.Field("chunks.text");
        f.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        innerHl.field(f);

        InnerHitBuilder inner = new InnerHitBuilder();
        inner.setHighlightBuilder(innerHl);

        NestedQueryBuilder nested = new NestedQueryBuilder("chunks", new MatchQueryBuilder("chunks.text", "x"), ScoreMode.Avg).innerHit(
            inner
        );
        source.query(nested);
        request.source(source);
        return request;
    }

    private static List<SearchExtBuilder> extEnabled(boolean enabled) {
        return List.of(new SemanticHighlighterExtBuilder(enabled));
    }
}
