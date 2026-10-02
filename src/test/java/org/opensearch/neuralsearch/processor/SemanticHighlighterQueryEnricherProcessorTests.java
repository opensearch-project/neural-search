/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SemanticHighlighterQueryEnricherProcessorTests extends OpenSearchTestCase {

    private static final String DEFAULT_MODEL_ID = "default-model-id";
    private static final String FIELD_MODEL_ID = "field-model-id";
    private static final String CONTENT_FIELD = "content";
    private static final String TITLE_FIELD = "title";
    private static final String SEMANTIC = SemanticHighlightingConstants.HIGHLIGHTER_TYPE;
    private static final String MODEL_ID = SemanticHighlightingConstants.MODEL_ID;

    @Before
    public void setup() {
        TestUtils.initializeEventStatsManager();
    }

    // ---------------------------------------------------------------------
    // Factory
    // ---------------------------------------------------------------------

    public void testFactory_whenNoModelIdAndNoFieldMap_thenThrowException() {
        SemanticHighlighterQueryEnricherProcessor.Factory factory = new SemanticHighlighterQueryEnricherProcessor.Factory();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, new HashMap<>(), null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("semantic_highlighter_field_default_id"));
    }

    public void testFactory_whenOnlyDefaultModelId_thenSuccess() throws Exception {
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertEquals(DEFAULT_MODEL_ID, processor.getModelId());
        assertNull(processor.getFieldDefaultIdMap());
    }

    public void testFactory_whenOnlyFieldMap_thenSuccess() throws Exception {
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(null, Map.of(CONTENT_FIELD, FIELD_MODEL_ID));
        assertNull(processor.getModelId());
        assertEquals(FIELD_MODEL_ID, processor.getFieldDefaultIdMap().get(CONTENT_FIELD));
    }

    public void testFactory_whenModelIdIsNotString_thenThrowException() {
        SemanticHighlighterQueryEnricherProcessor.Factory factory = new SemanticHighlighterQueryEnricherProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("default_model_id", 12345L);
        expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), null, null, false, config, null));
    }

    public void testFactory_whenFieldMapValueIsNotString_thenThrowException() {
        SemanticHighlighterQueryEnricherProcessor.Factory factory = new SemanticHighlighterQueryEnricherProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("semantic_highlighter_field_default_id", Map.of(CONTENT_FIELD, 12345L));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, config, null)
        );
        // The offending field must be named so the operator can find it in the pipeline config
        assertTrue(e.getMessage(), e.getMessage().contains(CONTENT_FIELD));
    }

    public void testType() throws Exception {
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertEquals(SemanticHighlightingConstants.QUERY_ENRICHER_TYPE, processor.getType());
    }

    // ---------------------------------------------------------------------
    // processRequest: degenerate requests
    // ---------------------------------------------------------------------

    /** A transport level request built without a body has a null source. */
    public void testProcessRequest_whenSourceIsNull_thenNoOp() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        assertNull(searchRequest.source());
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertSame(searchRequest, processor.processRequest(searchRequest));
    }

    public void testProcessRequest_whenEmptySource_thenNoOp() throws Exception {
        SearchRequest searchRequest = requestOf(new SearchSourceBuilder());
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        processor.processRequest(searchRequest);
        assertNull(searchRequest.source().highlighter());
        assertNull(searchRequest.source().query());
    }

    // ---------------------------------------------------------------------
    // processRequest: top level highlighter
    // ---------------------------------------------------------------------

    /** Global type: semantic with no options at all, the common case the processor exists for. */
    public void testProcessRequest_whenGlobalSemanticAndNoOptions_thenGlobalAndFieldEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        hlBuilder.highlighterType(SEMANTIC);
        hlBuilder.field(new HighlightBuilder.Field(CONTENT_FIELD));

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals(DEFAULT_MODEL_ID, enriched.options().get(MODEL_ID));
        assertEquals(DEFAULT_MODEL_ID, enriched.fields().get(0).options().get(MODEL_ID));
    }

    /** Field level type: semantic with no global type, and no options. */
    public void testProcessRequest_whenFieldSemanticAndNoOptions_thenFieldEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals(DEFAULT_MODEL_ID, enriched.fields().get(0).options().get(MODEL_ID));
        // The global highlighter is not semantic, it must not be touched
        assertNull(enriched.options());
    }

    /** A model_id supplied in the query always wins over the pipeline configuration. */
    public void testProcessRequest_whenModelIdAlreadySet_thenPreserved() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        Map<String, Object> options = new HashMap<>();
        options.put(MODEL_ID, "user-supplied-model-id");
        field.options(options);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals("user-supplied-model-id", enriched.fields().get(0).options().get(MODEL_ID));
    }

    /**
     * A model_id declared once in the global highlight options covers every field. Field level
     * options win when merged with the global ones, so enriching the field would silently
     * override the model the user asked for.
     */
    public void testProcessRequest_whenGlobalOptionsCarryModelId_thenFieldNotOverridden() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        Map<String, Object> globalOptions = new HashMap<>();
        globalOptions.put(MODEL_ID, "user-supplied-model-id");
        hlBuilder.options(globalOptions);
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals("user-supplied-model-id", enriched.options().get(MODEL_ID));
        assertNull("The pipeline default must not be pushed onto the field", enriched.fields().get(0).options());
    }

    /** A per field override from the pipeline must not beat a model_id supplied in the query either. */
    public void testProcessRequest_whenGlobalOptionsCarryModelIdAndFieldMapConfigured_thenFieldNotOverridden() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        Map<String, Object> globalOptions = new HashMap<>();
        globalOptions.put(MODEL_ID, "user-supplied-model-id");
        hlBuilder.options(globalOptions);
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, Map.of(CONTENT_FIELD, FIELD_MODEL_ID));

        assertEquals("user-supplied-model-id", enriched.options().get(MODEL_ID));
        assertNull("The pipeline field override must not beat the query's global model_id", enriched.fields().get(0).options());
    }

    /** Global type semantic with a user supplied global model_id: nothing to enrich anywhere. */
    public void testProcessRequest_whenGlobalSemanticAndGlobalModelIdSet_thenUntouched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        hlBuilder.highlighterType(SEMANTIC);
        Map<String, Object> globalOptions = new HashMap<>();
        globalOptions.put(MODEL_ID, "user-supplied-model-id");
        hlBuilder.options(globalOptions);
        hlBuilder.field(new HighlightBuilder.Field(CONTENT_FIELD));

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals("user-supplied-model-id", enriched.options().get(MODEL_ID));
        assertNull(enriched.fields().get(0).options());
    }

    /**
     * Global options that carry no model_id must not suppress enrichment. The guard keys on the
     * presence of model_id, not on the presence of a global options block.
     */
    public void testProcessRequest_whenGlobalOptionsWithoutModelId_thenStillEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        Map<String, Object> globalOptions = new HashMap<>();
        globalOptions.put("random_option", "some-value");
        hlBuilder.options(globalOptions);
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals(DEFAULT_MODEL_ID, enriched.fields().get(0).options().get(MODEL_ID));
        // The unrelated global option is left alone
        assertEquals("some-value", enriched.options().get("random_option"));
    }

    /** Enrichment must not drop options the user already set. */
    public void testProcessRequest_whenOtherOptionsSet_thenPreservedAlongsideModelId() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType(SEMANTIC);
        Map<String, Object> options = new HashMap<>();
        options.put("random_option", true);
        field.options(options);
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        Map<String, Object> enrichedOptions = enriched.fields().getFirst().options();
        assertEquals(DEFAULT_MODEL_ID, enrichedOptions.get(MODEL_ID));
        assertEquals(true, enrichedOptions.get("random_option"));
    }

    public void testProcessRequest_whenFieldIsNotSemantic_thenNotEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType("plain");
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertNull(enriched.fields().get(0).options());
    }

    /** A field that opts out of the semantic global type must not be enriched, the global one still is. */
    public void testProcessRequest_whenGlobalSemanticButFieldOverridesType_thenOnlyGlobalEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        hlBuilder.highlighterType(SEMANTIC);
        HighlightBuilder.Field field = new HighlightBuilder.Field(CONTENT_FIELD);
        field.highlighterType("plain");
        hlBuilder.field(field);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals(DEFAULT_MODEL_ID, enriched.options().get(MODEL_ID));
        assertNull(enriched.fields().get(0).options());
    }

    public void testProcessRequest_whenNoFieldsDeclared_thenGlobalStillEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        hlBuilder.highlighterType(SEMANTIC);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, null);

        assertEquals(DEFAULT_MODEL_ID, enriched.options().get(MODEL_ID));
        assertTrue(enriched.fields().isEmpty());
    }

    // ---------------------------------------------------------------------
    // processRequest: per field model id map
    // ---------------------------------------------------------------------

    public void testProcessRequest_whenFieldMapConfigured_thenFieldSpecificModelIdWins() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field content = new HighlightBuilder.Field(CONTENT_FIELD);
        content.highlighterType(SEMANTIC);
        hlBuilder.field(content);
        HighlightBuilder.Field title = new HighlightBuilder.Field(TITLE_FIELD);
        title.highlighterType(SEMANTIC);
        hlBuilder.field(title);

        HighlightBuilder enriched = process(hlBuilder, DEFAULT_MODEL_ID, Map.of(CONTENT_FIELD, FIELD_MODEL_ID));

        assertEquals(FIELD_MODEL_ID, enriched.fields().get(0).options().get(MODEL_ID));
        // No override for title, it falls back to the default model id
        assertEquals(DEFAULT_MODEL_ID, enriched.fields().get(1).options().get(MODEL_ID));
    }

    /** Only per field overrides configured, a field absent from the map has nothing to enrich with. */
    public void testProcessRequest_whenOnlyFieldMapAndFieldAbsent_thenNotEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        HighlightBuilder.Field title = new HighlightBuilder.Field(TITLE_FIELD);
        title.highlighterType(SEMANTIC);
        hlBuilder.field(title);

        HighlightBuilder enriched = process(hlBuilder, null, Map.of(CONTENT_FIELD, FIELD_MODEL_ID));

        assertNull(enriched.fields().get(0).options());
    }

    /** Only per field overrides configured, the global highlighter has no default to fall back on. */
    public void testProcessRequest_whenOnlyFieldMapAndGlobalSemantic_thenGlobalNotEnriched() throws Exception {
        HighlightBuilder hlBuilder = new HighlightBuilder();
        hlBuilder.highlighterType(SEMANTIC);
        hlBuilder.field(new HighlightBuilder.Field(CONTENT_FIELD));

        HighlightBuilder enriched = process(hlBuilder, null, Map.of(CONTENT_FIELD, FIELD_MODEL_ID));

        assertNull(enriched.options());
        assertEquals(FIELD_MODEL_ID, enriched.fields().get(0).options().get(MODEL_ID));
    }

    // ---------------------------------------------------------------------
    // processRequest: nested inner_hits
    // ---------------------------------------------------------------------

    public void testProcessRequest_whenNestedInnerHitsHighlight_thenEnriched() throws Exception {
        NestedQueryBuilder nested = nestedWithSemanticInnerHit("chunks", "chunks.text");

        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().query(nested));
        createProcessor(DEFAULT_MODEL_ID, null).processRequest(searchRequest);

        assertEquals(DEFAULT_MODEL_ID, innerHitOptions(nested).get(MODEL_ID));
    }

    /** The realistic shape: a nested clause wrapped in a bool, reached through getChildVisitor. */
    public void testProcessRequest_whenNestedInsideBool_thenEnriched() throws Exception {
        NestedQueryBuilder nested = nestedWithSemanticInnerHit("chunks", "chunks.text");

        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().query(new BoolQueryBuilder().must(nested)));
        createProcessor(DEFAULT_MODEL_ID, null).processRequest(searchRequest);

        assertEquals(DEFAULT_MODEL_ID, innerHitOptions(nested).get(MODEL_ID));
    }

    public void testProcessRequest_whenNestedWithoutInnerHits_thenNoOp() throws Exception {
        NestedQueryBuilder nested = new NestedQueryBuilder("chunks", new MatchQueryBuilder("chunks.text", "x"), ScoreMode.Avg);
        assertNull(nested.innerHit());

        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().query(nested));
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertSame(searchRequest, processor.processRequest(searchRequest));
    }

    public void testProcessRequest_whenInnerHitsWithoutHighlight_thenNoOp() throws Exception {
        InnerHitBuilder innerHit = new InnerHitBuilder();
        assertNull(innerHit.getHighlightBuilder());
        NestedQueryBuilder nested = new NestedQueryBuilder("chunks", new MatchQueryBuilder("chunks.text", "x"), ScoreMode.Avg).innerHit(
            innerHit
        );

        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().query(nested));
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertSame(searchRequest, processor.processRequest(searchRequest));
    }

    public void testProcessRequest_whenNonNestedQuery_thenNoOp() throws Exception {
        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().query(new MatchQueryBuilder(CONTENT_FIELD, "x")));
        SemanticHighlighterQueryEnricherProcessor processor = createProcessor(DEFAULT_MODEL_ID, null);
        assertSame(searchRequest, processor.processRequest(searchRequest));
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /** Runs the processor over a request carrying only the given highlighter, and returns it for assertions. */
    private HighlightBuilder process(HighlightBuilder hlBuilder, String modelId, Map<String, String> fieldMap) throws Exception {
        SearchRequest searchRequest = requestOf(new SearchSourceBuilder().highlighter(hlBuilder));
        createProcessor(modelId, fieldMap).processRequest(searchRequest);
        return searchRequest.source().highlighter();
    }

    /** A nested query whose inner_hits declare a single {@code type: semantic} field. */
    private NestedQueryBuilder nestedWithSemanticInnerHit(String path, String fieldName) {
        HighlightBuilder innerHighlight = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field(fieldName);
        field.highlighterType(SEMANTIC);
        innerHighlight.field(field);

        InnerHitBuilder innerHit = new InnerHitBuilder();
        innerHit.setHighlightBuilder(innerHighlight);
        return new NestedQueryBuilder(path, new MatchQueryBuilder(fieldName, "x"), ScoreMode.Avg).innerHit(innerHit);
    }

    private static Map<String, Object> innerHitOptions(NestedQueryBuilder nested) {
        return nested.innerHit().getHighlightBuilder().fields().get(0).options();
    }

    private SearchRequest requestOf(SearchSourceBuilder source) {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(source);
        return searchRequest;
    }

    private SemanticHighlighterQueryEnricherProcessor createProcessor(String modelId, Map<String, String> fieldMap) throws Exception {
        SemanticHighlighterQueryEnricherProcessor.Factory factory = new SemanticHighlighterQueryEnricherProcessor.Factory();
        // readOptionalMap/readOptionalStringProperty remove consumed keys, so the config map must be mutable
        Map<String, Object> config = new HashMap<>();
        if (modelId != null) {
            config.put("default_model_id", modelId);
        }
        if (fieldMap != null) {
            config.put("semantic_highlighter_field_default_id", new HashMap<String, Object>(fieldMap));
        }
        return factory.create(Collections.emptyMap(), null, null, false, config, null);
    }
}
