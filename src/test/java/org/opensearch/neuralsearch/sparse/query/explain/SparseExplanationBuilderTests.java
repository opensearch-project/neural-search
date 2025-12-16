/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query.explain;

import lombok.SneakyThrows;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.query.SparseQueryContext;
import org.opensearch.neuralsearch.sparse.query.SparseVectorQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;

public class SparseExplanationBuilderTests extends AbstractSparseTestBase {

    private static final String FIELD_NAME = "test_field";
    private static final int DOC_ID = 5;
    private static final float BOOST = 1.0f;

    @Mock
    private LeafReaderContext mockContext;
    @Mock
    private SparseVectorQuery mockQuery;
    @Mock
    private FieldInfo mockFieldInfo;
    @Mock
    private SparseVectorReader mockReader;
    @Mock
    private SparseQueryContext mockQueryContext;

    private SparseVector queryVector;
    private SparseVector docVector;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Setup default mocks
        when(mockQuery.getFieldName()).thenReturn(FIELD_NAME);
        when(mockQuery.getQueryContext()).thenReturn(mockQueryContext);
        when(mockQueryContext.getTokens()).thenReturn(List.of("1", "2", "3"));

        // Create query and document vectors
        queryVector = createVector(1, 100, 2, 89, 3, 47);
        docVector = createVector(1, 100, 2, 89, 3, 202);

        when(mockQuery.getQueryVector()).thenReturn(queryVector);

        // Setup field info with quantization parameters
        Map<String, String> attributes = new HashMap<>();
        attributes.put(QUANTIZATION_CEILING_INGEST_FIELD, "3.0");
        attributes.put(QUANTIZATION_CEILING_SEARCH_FIELD, "16.0");
        when(mockFieldInfo.attributes()).thenReturn(attributes);
        when(mockFieldInfo.getAttribute(QUANTIZATION_CEILING_INGEST_FIELD)).thenReturn("3.0");
        when(mockFieldInfo.getAttribute(QUANTIZATION_CEILING_SEARCH_FIELD)).thenReturn("16.0");
        when(mockFieldInfo.getIndexOptions()).thenReturn(IndexOptions.DOCS_AND_FREQS);
    }

    public void testExplain_BasicScoring() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());
        assertTrue("Explanation should have details", explanation.getDetails().length > 0);
        assertTrue("Explanation description should contain field name", explanation.getDescription().contains(FIELD_NAME));
        assertTrue("Explanation description should contain doc ID", explanation.getDescription().contains(String.valueOf(DOC_ID)));
    }

    public void testExplain_InvalidDocumentId() throws IOException {
        SparseExplanationBuilder builder = SparseExplanationBuilder.builder()
            .context(mockContext)
            .doc(-1)
            .query(mockQuery)
            .boost(BOOST)
            .fieldInfo(mockFieldInfo)
            .reader(mockReader)
            .build();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertFalse("Explanation should not match for invalid doc ID", explanation.isMatch());
        assertTrue("Explanation should mention invalid document ID", explanation.getDescription().contains("invalid document ID"));
    }

    public void testExplain_EmptyQueryVector() throws IOException {
        SparseVector emptyVector = createVector();
        when(mockQuery.getQueryVector()).thenReturn(emptyVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertFalse("Explanation should not match for empty query vector", explanation.isMatch());
        assertTrue("Explanation should mention empty query vector", explanation.getDescription().contains("empty"));
    }

    public void testExplain_DocumentNotFound() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(null);

        SparseExplanationBuilder builder = SparseExplanationBuilder.builder()
                .context(mockContext)
                .doc(DOC_ID)
                .query(mockQuery)
                .boost(BOOST)
                .fieldInfo(mockFieldInfo)
                .reader(mockReader)
                .build();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertFalse("Explanation should not match when document not found", explanation.isMatch());
        assertTrue("Explanation should mention document not found", explanation.getDescription().contains("not found"));
    }

    public void testExplain_IOExceptionDuringRead() throws IOException {
        when(mockReader.read(DOC_ID)).thenThrow(new IOException("Test IO error"));

        SparseExplanationBuilder builder = SparseExplanationBuilder.builder()
                .context(mockContext)
                .doc(DOC_ID)
                .query(mockQuery)
                .boost(BOOST)
                .fieldInfo(mockFieldInfo)
                .reader(mockReader)
                .build();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertFalse("Explanation should not match on IO error", explanation.isMatch());
        assertTrue("Explanation should mention error", explanation.getDescription().contains("error"));
    }

    public void testExplain_WithQueryPruning() throws IOException {
        // Create a larger query vector to simulate pruning
        SparseVector largeQueryVector = createVector(1, 100, 2, 89, 3, 47, 4, 30, 5, 20);
        when(mockQuery.getQueryVector()).thenReturn(largeQueryVector);
        when(mockQueryContext.getTokens()).thenReturn(List.of("1", "2", "3")); // Only 3 tokens kept

        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the pruning explanation
        boolean foundPruningExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("query token pruning")) {
                foundPruningExplanation = true;
                assertTrue("Pruning explanation should mention kept tokens", detail.getDescription().contains("kept top 3 of 5"));
                break;
            }
        }
        assertTrue("Should have pruning explanation", foundPruningExplanation);
    }

    public void testExplain_NoPruning() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the pruning explanation
        boolean foundNoPruningMessage = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("no pruning occurred")) {
                foundNoPruningMessage = true;
                break;
            }
        }
        assertTrue("Should indicate no pruning occurred", foundNoPruningMessage);
    }

    public void testExplain_TokenContributions() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the raw score explanation
        boolean foundRawScoreExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("raw dot product score")) {
                foundRawScoreExplanation = true;
                assertTrue("Raw score explanation should have token details", detail.getDetails().length > 0);

                for (Explanation tokenDetail : detail.getDetails()) {
                    assertTrue(
                        "Token detail should show contribution",
                        tokenDetail.getDescription().contains("contribution") || tokenDetail.getDescription().contains("query_weight")
                    );
                }
                break;
            }
        }
        assertTrue("Should have raw score explanation", foundRawScoreExplanation);
    }

    public void testExplain_QuantizationRescaling() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Calculate expected rescaled boost
        float ceilingIngest = 3.0f;
        float ceilingSearch = 16.0f;
        float expectedRescaledBoost = BOOST * ceilingIngest * ceilingSearch / 255.0f / 255.0f;

        // Find the quantization rescaling explanation
        boolean foundRescalingExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("quantization rescaling")) {
                foundRescalingExplanation = true;
                assertTrue("Rescaling explanation should have details", detail.getDetails().length > 0);

                float actualRescaledBoost = detail.getValue().floatValue();
                assertEquals(
                    "Rescaled boost should match expected calculation",
                    expectedRescaledBoost,
                    actualRescaledBoost,
                    0.000001f
                );

                // Check for expected sub-details and their values
                boolean foundBoost = false;
                boolean foundCeilingIngest = false;
                boolean foundCeilingSearch = false;
                boolean foundMaxValue = false;

                for (Explanation subDetail : detail.getDetails()) {
                    String desc = subDetail.getDescription();
                    if (desc.contains("original boost")) {
                        foundBoost = true;
                        assertEquals("Boost value should match", BOOST, subDetail.getValue().floatValue(), 0.0001f);
                    }
                    if (desc.contains("ceiling_ingest")) {
                        foundCeilingIngest = true;
                        assertEquals("Ceiling ingest should match", ceilingIngest, subDetail.getValue().floatValue(), 0.01f);
                    }
                    if (desc.contains("ceiling_search")) {
                        foundCeilingSearch = true;
                        assertEquals("Ceiling search should match", ceilingSearch, subDetail.getValue().floatValue(), 0.01f);
                    }
                    if (desc.contains("MAX_UNSIGNED_BYTE_VALUE")) {
                        foundMaxValue = true;
                        assertEquals("Max unsigned byte value should be 255", 255, subDetail.getValue().intValue());
                    }
                }

                assertTrue("Should have boost explanation", foundBoost);
                assertTrue("Should have ceiling_ingest explanation", foundCeilingIngest);
                assertTrue("Should have ceiling_search explanation", foundCeilingSearch);
                assertTrue("Should have MAX_UNSIGNED_BYTE_VALUE explanation", foundMaxValue);
                break;
            }
        }
        assertTrue("Should have rescaling explanation", foundRescalingExplanation);
    }

    public void testExplain_WithFilter_ExactSearchMode() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        Query mockFilterQuery = mock(TermQuery.class);
        when(mockFilterQuery.toString()).thenReturn("field:value");
        when(mockQuery.getFilter()).thenReturn(mockFilterQuery);

        // Setup filter results with 3 documents passing filter
        Map<Object, BitSet> filterResults = new HashMap<>();
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(DOC_ID); // Document passes filter
        bitSet.set(3);
        bitSet.set(7);
        String contextId = "test-context";
        when(mockContext.id()).thenReturn(contextId);
        filterResults.put(contextId, bitSet);
        when(mockQuery.getFilterResults()).thenReturn(filterResults);

        // Set k=5, P=3, so P <= k triggers exact search mode
        when(mockQueryContext.getK()).thenReturn(5);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the filter explanation
        boolean foundFilterExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("filter")) {
                foundFilterExplanation = true;
                assertTrue("Filter explanation should indicate document passed", detail.getDescription().contains("passed filter"));
                assertTrue("Filter explanation should indicate exact search mode", detail.getDescription().contains("exact search mode"));
                assertTrue("Filter explanation should show P <= k relationship", detail.getDescription().contains("3 documents <= k=5"));
                assertTrue(
                    "Filter explanation should mention exact scoring",
                    detail.getDescription().contains("all filtered documents scored exactly")
                );
                break;
            }
        }
        assertTrue("Should have filter explanation", foundFilterExplanation);
    }

    public void testExplain_WithFilter_ApproximateSearchMode() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        Query mockFilterQuery = mock(TermQuery.class);
        when(mockFilterQuery.toString()).thenReturn("field:value");
        when(mockQuery.getFilter()).thenReturn(mockFilterQuery);

        // Setup filter results with 5 documents passing filter
        Map<Object, BitSet> filterResults = new HashMap<>();
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(DOC_ID); // Document passes filter
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(7);
        bitSet.set(9);
        String contextId = "test-context";
        when(mockContext.id()).thenReturn(contextId);
        filterResults.put(contextId, bitSet);
        when(mockQuery.getFilterResults()).thenReturn(filterResults);

        // Set k=3, P=5, so P > k triggers approximate search mode with post-filtering
        when(mockQueryContext.getK()).thenReturn(3);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the filter explanation
        boolean foundFilterExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("filter")) {
                foundFilterExplanation = true;
                assertTrue("Filter explanation should indicate document passed", detail.getDescription().contains("passed filter"));
                assertTrue(
                    "Filter explanation should indicate approximate search mode",
                    detail.getDescription().contains("approximate search mode")
                );
                assertTrue("Filter explanation should show P > k relationship", detail.getDescription().contains("5 documents > k=3"));
                assertTrue(
                    "Filter explanation should mention ANN search then filter",
                    detail.getDescription().contains("ANN search performed first then filtered")
                );
                break;
            }
        }
        assertTrue("Should have filter explanation", foundFilterExplanation);
    }

    public void testExplain_WithFilter_DocumentFiltered() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        Query mockFilterQuery = mock(TermQuery.class);
        when(mockFilterQuery.toString()).thenReturn("field:value");
        when(mockQuery.getFilter()).thenReturn(mockFilterQuery);

        Map<Object, BitSet> filterResults = new HashMap<>();
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(3);
        String contextId = "test-context";
        when(mockContext.id()).thenReturn(contextId);
        filterResults.put(contextId, bitSet);
        when(mockQuery.getFilterResults()).thenReturn(filterResults);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match (filter doesn't affect main score)", explanation.isMatch());

        // Find the filter explanation
        boolean foundFilterExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("filter")) {
                foundFilterExplanation = true;
                assertTrue("Filter explanation should indicate document was filtered", detail.getDescription().contains("filtered out"));
                break;
            }
        }
        assertTrue("Should have filter explanation", foundFilterExplanation);
    }

    public void testExplain_WithFilter_NoFilterResults() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        Query mockFilterQuery = mock(TermQuery.class);
        when(mockQuery.getFilter()).thenReturn(mockFilterQuery);
        when(mockQuery.getFilterResults()).thenReturn(null);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the filter explanation
        boolean foundFilterExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("filter")) {
                foundFilterExplanation = true;
                assertTrue("Filter explanation should mention no results available", detail.getDescription().contains("no filter results"));
                break;
            }
        }
        assertTrue("Should have filter explanation", foundFilterExplanation);
    }

    public void testExplain_WithFilter_NoBitSetForSegment() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        Query mockFilterQuery = mock(TermQuery.class);
        when(mockQuery.getFilter()).thenReturn(mockFilterQuery);

        Map<Object, BitSet> filterResults = new HashMap<>();
        String contextId = "test-context";
        when(mockContext.id()).thenReturn(contextId);
        when(mockQuery.getFilterResults()).thenReturn(filterResults);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the filter explanation
        boolean foundFilterExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("filter")) {
                foundFilterExplanation = true;
                assertTrue(
                    "Filter explanation should indicate no documents matched in segment",
                    detail.getDescription().contains("no documents in segment matched filter")
                );
                break;
            }
        }
        assertTrue("Should have filter explanation", foundFilterExplanation);
    }

    public void testExplain_CustomBoost() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        float customBoost = 2.5f;
        SparseExplanationBuilder builder = SparseExplanationBuilder.builder()
            .context(mockContext)
            .doc(DOC_ID)
            .query(mockQuery)
            .boost(customBoost)
            .fieldInfo(mockFieldInfo)
            .reader(mockReader)
            .build();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the quantization rescaling explanation and verify boost
        boolean foundBoostValue = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("quantization rescaling")) {
                for (Explanation subDetail : detail.getDetails()) {
                    if (subDetail.getDescription().contains("original boost")) {
                        assertEquals("Boost value should match", customBoost, subDetail.getValue().floatValue(), 0.0001f);
                        foundBoostValue = true;
                        break;
                    }
                }
                break;
            }
        }
        assertTrue("Should find boost value in explanation", foundBoostValue);
    }

    public void testExplain_ScoreCalculation() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Raw score = 100*100 + 89*89 + 47*202 = 10000 + 7921 + 9494 = 27415
        int expectedRawScore = 10000 + 7921 + 9494;

        // Rescaled boost = 1.0 * 3.0 * 16.0 / 255 / 255 = 48.0 / 65025 ≈ 0.0007381776
        float ceilingIngest = 3.0f;
        float ceilingSearch = 16.0f;
        float expectedRescaledBoost = BOOST * ceilingIngest * ceilingSearch / 255.0f / 255.0f;

        // Final score = rawScore * rescaledBoost
        float expectedFinalScore = expectedRawScore * expectedRescaledBoost;

        // Verify the final score matches expected calculation
        float actualScore = explanation.getValue().floatValue();
        assertEquals("Final score should match expected calculation", expectedFinalScore, actualScore, 0.0001f);

        // Also verify the raw score in the explanation details
        boolean foundRawScore = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("raw dot product score")) {
                foundRawScore = true;
                int actualRawScore = detail.getValue().intValue();
                assertEquals("Raw score should match expected calculation", expectedRawScore, actualRawScore);
                break;
            }
        }
        assertTrue("Should have raw score explanation", foundRawScore);
    }

    public void testExplain_TokenIdOutOfBounds() throws IOException {
        // Create a query vector with tokens that are out of bounds
        SparseVector smallQueryVector = createVector(1, 100, 2, 89);
        when(mockQuery.getQueryVector()).thenReturn(smallQueryVector);

        // Query context includes token IDs that exceed the query vector length
        when(mockQueryContext.getTokens()).thenReturn(List.of("1", "2", "10", "20")); // 10 and 20 are out of bounds

        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the raw score explanation and verify only valid tokens are included
        boolean foundRawScoreExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("raw dot product score")) {
                foundRawScoreExplanation = true;

                // Check that out-of-bounds tokens are not in the explanation
                for (Explanation tokenDetail : detail.getDetails()) {
                    String desc = tokenDetail.getDescription();
                    assertFalse("Should not include out-of-bounds token 10", desc.contains("token '10'"));
                    assertFalse("Should not include out-of-bounds token 20", desc.contains("token '20'"));
                }
                break;
            }
        }
        assertTrue("Should have raw score explanation", foundRawScoreExplanation);
    }

    public void testExplain_ZeroWeightTokens() throws IOException {
        // Create a query vector where some tokens have zero weight
        SparseVector queryVectorWithZeros = createVector(1, 100, 2, 0, 3, 47, 4, 0);
        when(mockQuery.getQueryVector()).thenReturn(queryVectorWithZeros);
        when(mockQueryContext.getTokens()).thenReturn(List.of("1", "2", "3", "4"));

        // Create doc vector with all tokens present
        SparseVector docVectorAll = createVector(1, 100, 2, 89, 3, 202, 4, 50);
        when(mockReader.read(DOC_ID)).thenReturn(docVectorAll);

        SparseExplanationBuilder builder = createDefaultBuilder();

        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the raw score explanation and verify zero-weight tokens are not included
        boolean foundRawScoreExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("raw dot product score")) {
                foundRawScoreExplanation = true;

                // Check that zero-weight tokens are not in the explanation
                boolean foundToken2 = false;
                boolean foundToken4 = false;
                for (Explanation tokenDetail : detail.getDetails()) {
                    String desc = tokenDetail.getDescription();
                    if (desc.contains("token '2'")) foundToken2 = true;
                    if (desc.contains("token '4'")) foundToken4 = true;
                }

                assertFalse("Should not include zero-weight token 2", foundToken2);
                assertFalse("Should not include zero-weight token 4", foundToken4);
                break;
            }
        }
        assertTrue("Should have raw score explanation", foundRawScoreExplanation);
    }

    public void testExplain_InvalidTokenIdFormat() throws IOException {
        // Query context includes invalid token IDs (non-numeric strings)
        when(mockQueryContext.getTokens()).thenReturn(List.of("1", "invalid", "abc", "2", "-5"));

        when(mockReader.read(DOC_ID)).thenReturn(docVector);

        SparseExplanationBuilder builder = createDefaultBuilder();

        // Should not throw exception, invalid tokens should be skipped gracefully
        Explanation explanation = builder.explain();

        assertNotNull("Explanation should not be null", explanation);
        assertTrue("Explanation should match", explanation.isMatch());

        // Find the raw score explanation and verify invalid tokens are not included
        boolean foundRawScoreExplanation = false;
        for (Explanation detail : explanation.getDetails()) {
            if (detail.getDescription().contains("raw dot product score")) {
                foundRawScoreExplanation = true;

                // Check that invalid tokens are not in the explanation
                for (Explanation tokenDetail : detail.getDetails()) {
                    String desc = tokenDetail.getDescription();
                    assertFalse("Should not include invalid token 'invalid'", desc.contains("token 'invalid'"));
                    assertFalse("Should not include invalid token 'abc'", desc.contains("token 'abc'"));
                    assertFalse("Should not include negative token '-5'", desc.contains("token '-5'"));
                }
                break;
            }
        }
        assertTrue("Should have raw score explanation", foundRawScoreExplanation);
    }

    private SparseExplanationBuilder createDefaultBuilder() throws IOException {
        when(mockReader.read(DOC_ID)).thenReturn(docVector);
        return SparseExplanationBuilder.builder()
                .context(mockContext)
                .doc(DOC_ID)
                .query(mockQuery)
                .boost(BOOST)
                .fieldInfo(mockFieldInfo)
                .reader(mockReader)
                .build();
    }
}
