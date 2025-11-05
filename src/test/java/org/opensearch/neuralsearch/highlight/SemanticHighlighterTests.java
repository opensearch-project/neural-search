/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.highlight.single.SemanticHighlighterEngine;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TestUtils;
import org.apache.lucene.search.Query;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants.HIGHLIGHTER_TYPE;

public class SemanticHighlighterTests extends OpenSearchTestCase {

    private SemanticHighlighter highlighter;

    @Mock
    private SemanticHighlighterEngine semanticHighlighterEngine;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Metadata metadata;

    private FieldHighlightContext fieldContext;

    @Mock
    private FetchContext fetchContext;

    @Mock
    private Query query;

    @Mock
    private MappedFieldType mappedFieldType;

    @Mock
    private SearchHighlightContext.Field field;

    @Mock
    private SearchHighlightContext.FieldOptions fieldOptions;

    @Mock
    private Settings settings;

    @Mock
    private FetchSubPhase.HitContext hitContext;

    @Mock
    private SourceLookup sourceLookup;

    @Mock
    private SearchPipelineService searchPipelineService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        TestUtils.initializeEventStatsManager();
        highlighter = new SemanticHighlighter();

        // Initialize NeuralSearchClusterUtil with mocked services
        NeuralSearchClusterUtil.instance().initialize(clusterService, null);
        NeuralSearchClusterUtil.instance().setSearchPipelineService(searchPipelineService);

        // Setup common mocks using reflection to set final fields
        fieldContext = mock(FieldHighlightContext.class);
        field = mock(SearchHighlightContext.Field.class);
        fieldOptions = mock(SearchHighlightContext.FieldOptions.class);
        hitContext = mock(FetchSubPhase.HitContext.class);
        sourceLookup = mock(SourceLookup.class);

        // Create FieldHighlightContext using constructor instead of reflection
        fieldContext = new FieldHighlightContext(
            "test_field",
            field,
            mappedFieldType,
            fetchContext,
            hitContext,
            query,
            false,
            new HashMap<>()
        );

        // Setup hitContext to return sourceLookup
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);
        // Setup sourceLookup to return test field content
        when(sourceLookup.extractValue("test_field", null)).thenReturn("test field content");

        when(field.fieldOptions()).thenReturn(fieldOptions);
        when(fieldOptions.preTags()).thenReturn(new String[] { "<em>" });
        when(fieldOptions.postTags()).thenReturn(new String[] { "</em>" });
    }

    public void testCanHighlightAlwaysReturnsTrue() {
        // Test with any field type - should always return true
        MappedFieldType fieldType = mock(MappedFieldType.class);
        assertTrue(highlighter.canHighlight(fieldType));

        // Test with null - should still return true
        assertTrue(highlighter.canHighlight(null));
    }

    public void testHighlighterName() {
        // Verify the highlighter name matches the constant
        assertEquals("semantic", HIGHLIGHTER_TYPE);
    }

    public void testInitializeThrowsExceptionWhenAlreadyInitialized() {
        highlighter.initialize(semanticHighlighterEngine);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> highlighter.initialize(semanticHighlighterEngine)
        );

        assertTrue(exception.getMessage().contains("already been initialized"));
    }

    // ============= Tests for Single Inference Mode =============

    public void testSingleInferenceModeWithoutBatchInference() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        when(semanticHighlighterEngine.extractOriginalQuery(any(), anyString())).thenReturn("test query");
        when(semanticHighlighterEngine.getHighlightedSentences(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(
            "<em>highlighted text</em>"
        );

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNotNull(result);
        assertEquals("test_field", result.name());
        assertEquals(1, result.fragments().length);
        assertEquals("<em>highlighted text</em>", result.fragments()[0].string());
    }

    public void testSingleInferenceModeWithBatchInferenceFalse() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", false);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        when(semanticHighlighterEngine.extractOriginalQuery(any(), anyString())).thenReturn("test query");
        when(semanticHighlighterEngine.getHighlightedSentences(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(
            "<em>highlighted text</em>"
        );

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNotNull(result);
        verify(semanticHighlighterEngine, times(1)).getHighlightedSentences(any(), any(), any(), any(), any());
    }

    public void testSingleInferenceModeReturnsNullWhenNoQueryText() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        when(semanticHighlighterEngine.extractOriginalQuery(any(), anyString())).thenReturn(null);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNull(result);
        verify(semanticHighlighterEngine, never()).getHighlightedSentences(any(), any(), any(), any(), any());
    }

    public void testSingleInferenceModeThrowsExceptionWhenNotInitialized() {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        when(fieldOptions.options()).thenReturn(options);

        // Execute & Verify
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> highlighter.highlight(fieldContext));

        assertTrue(exception.getMessage().contains("SemanticHighlighter has not been initialized"));
    }

    // ============= Tests for Batch Inference Mode =============

    public void testBatchInferenceModeWithSystemProcessorEnabled() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor enabled
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(true);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNull(result); // Should return null to defer to processor
        verify(semanticHighlighterEngine, never()).getHighlightedSentences(any(), any(), any(), any(), any());
    }

    public void testBatchInferenceModeThrowsExceptionWhenSystemProcessorDisabled() {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor disabled
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(false);

        // Execute & Verify
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(fieldContext));

        // Updated assertions to match new error message
        assertTrue(exception.getMessage().contains("Batch inference for semantic highlighting is disabled"));
        assertTrue(exception.getMessage().contains("Enable it by adding"));
        assertTrue(exception.getMessage().contains("semantic-highlighter"));
        assertTrue(exception.getMessage().contains("cluster.search.enabled_system_generated_factories"));
    }

    public void testBatchInferenceModeWithStringValue() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", "true"); // String instead of boolean
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor enabled
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(true);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNull(result); // Should return null to defer to processor
    }

    // ============= Tests for Custom Tags =============

    public void testCustomPreAndPostTags() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        when(fieldOptions.options()).thenReturn(options);
        when(fieldOptions.preTags()).thenReturn(new String[] { "<mark>" });
        when(fieldOptions.postTags()).thenReturn(new String[] { "</mark>" });

        highlighter.initialize(semanticHighlighterEngine);

        when(semanticHighlighterEngine.extractOriginalQuery(any(), anyString())).thenReturn("test query");
        when(semanticHighlighterEngine.getHighlightedSentences(anyString(), anyString(), anyString(), eq("<mark>"), eq("</mark>")))
            .thenReturn("<mark>highlighted text</mark>");

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNotNull(result);
        assertEquals("<mark>highlighted text</mark>", result.fragments()[0].string());

        verify(semanticHighlighterEngine, times(1)).getHighlightedSentences(
            eq("test_model"),
            eq("test query"),
            anyString(),
            eq("<mark>"),
            eq("</mark>")
        );
    }

    // ============= Edge Case Tests =============

    public void testSystemProcessorEnabledWithOtherFactories() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor enabled among other factories
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(true);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNull(result); // Should return null to defer to processor
    }

    public void testSingleInferenceModeReturnsNullWhenEmptyHighlightedText() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        when(semanticHighlighterEngine.extractOriginalQuery(any(), anyString())).thenReturn("test query");
        when(semanticHighlighterEngine.getHighlightedSentences(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(
            ""
        );

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify
        assertNull(result);
    }

    // ============= Tests for Wildcard Support =============

    public void testSystemProcessorEnabledWithWildcard() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor enabled with wildcard "*"
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(true);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify - should return null to defer to processor
        assertNull(result);
        verify(semanticHighlighterEngine, never()).getHighlightedSentences(any(), any(), any(), any(), any());
    }

    public void testSystemProcessorEnabledWithWildcardAndOthers() throws Exception {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor enabled with wildcard and other factories
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(true);

        // Execute
        HighlightField result = highlighter.highlight(fieldContext);

        // Verify - should return null to defer to processor
        assertNull(result);
        verify(semanticHighlighterEngine, never()).getHighlightedSentences(any(), any(), any(), any(), any());
    }

    public void testSystemProcessorDisabledWithOtherFactoriesOnly() {
        // Setup
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", "test_model");
        options.put("batch_inference", true);
        when(fieldOptions.options()).thenReturn(options);

        highlighter.initialize(semanticHighlighterEngine);

        // Mock system processor with only other factories (no semantic-highlighter, no wildcard)
        when(searchPipelineService.isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE)).thenReturn(false);

        // Execute & Verify - should throw exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(fieldContext));

        assertTrue(exception.getMessage().contains("Batch inference for semantic highlighting is disabled"));
        assertTrue(exception.getMessage().contains("Enable it by adding"));
    }
}
