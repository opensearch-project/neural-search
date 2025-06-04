/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.Version;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.neuralsearch.query.dto.NeuralQueryBuildStage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils.setUpClusterService;

public class NeuralQueryBuilderBuilderTests extends OpenSearchTestCase {
    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String IMAGE_TEXT = "base641234567890";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final Integer K = 10;
    private static final Float MAX_DISTANCE = 1.0f;
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";
    private static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[10];
    private static final Supplier<Map<String, Float>> TEST_QUERY_TOKENS_MAP_SUPPLIER = () -> Map.of("key", 1.0f);
    private static final String TEST_SEMANTIC_FIELD_SEARCH_ANALYZER = "standard";
    private static final QueryBuilder TEST_FILTER = new MatchAllQueryBuilder();

    public void testBuilderInstantiation_whenMissingFieldName_thenFail() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder().queryText(QUERY_TEXT).modelId(MODEL_ID).k(K).build()
        );
        assertEquals("Field name must be provided for neural query", exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringFromXContentWithRequiredFields_thenBuildSuccessfully() {
        setUpClusterService();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName(FIELD_NAME).build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
    }

    public void testBuilderInstantiation_whenBuiltDuringFromXContentWithErrors_v3_0_0_thenFail() {
        setUpClusterService(Version.V_3_0_0);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder().fieldName(FIELD_NAME).k(K).maxDistance(MAX_DISTANCE).build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Either query_text or query_image must"
            + " be provided.; Only one of k, max_distance, or min_score can be provided";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringFromXContentWithErrors_v2_10_0_thenFail() {
        setUpClusterService(Version.V_2_10_0);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder().fieldName(FIELD_NAME).k(K).maxDistance(MAX_DISTANCE).build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Either query_text or query_image must"
            + " be provided.; Only one of k, max_distance, or min_score can be provided; model_id must be provided.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltWithTokensDuringFromXContentWithErrors_v2_10_0_thenFail() {
        setUpClusterService(Version.V_2_10_0);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder().fieldName(FIELD_NAME).queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER).build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Either query_text or query_image must "
            + "be provided.; model_id must be provided.; Target field is a KNN field using a dense model. "
            + "query_tokens is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithRequiredFieldsForKnn_thenBuildSuccessfully() {
        setUpClusterService();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(NeuralQueryBuilder.DEFAULT_K, (int) neuralQueryBuilder.k());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithRequiredFieldsForSemanticKnn_thenBuildSuccessfully() {
        setUpClusterService();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .isSemanticField(true)
            .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(NeuralQueryBuilder.DEFAULT_K, (int) neuralQueryBuilder.k());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithRequiredFieldsForSemanticRankFeatures_thenBuildSuccessfully() {
        setUpClusterService();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .isSemanticField(true)
            .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithOptionalFieldsForKnn_thenBuildSuccessfully() {
        setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .queryImage(IMAGE_TEXT)
            .modelId(MODEL_ID)
            .k(K)
            .expandNested(Boolean.TRUE)
            .boost(BOOST)
            .filter(TEST_FILTER)
            .vectorSupplier(TEST_VECTOR_SUPPLIER)
            .queryName(QUERY_NAME)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(IMAGE_TEXT, neuralQueryBuilder.queryImage());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(TEST_FILTER, neuralQueryBuilder.filter());
        assertEquals(TEST_VECTOR_SUPPLIER, neuralQueryBuilder.vectorSupplier());
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
        assertEquals(Boolean.TRUE, neuralQueryBuilder.expandNested());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithOptionalFieldsForRankFeatures_thenBuildSuccessfully() {
        setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
            .queryText(QUERY_TEXT)
            .queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER)
            .boost(BOOST)
            .queryName(QUERY_NAME)
            .isSemanticField(true)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(TEST_QUERY_TOKENS_MAP_SUPPLIER, neuralQueryBuilder.queryTokensMapSupplier());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithKnnFieldsForRankFeatures_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .queryImage(IMAGE_TEXT)
                .queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER)
                .modelId(MODEL_ID)
                .k(K)
                .expandNested(Boolean.TRUE)
                .boost(BOOST)
                .filter(TEST_FILTER)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: query_tokens, model_id and "
            + "semantic_field_search_analyzer can not coexist; Target field is a semantic field using a sparse model. "
            + "[filter, query_image, k, expand_nested_docs] are not supported since they are for the dense model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithRankFeaturesFieldsForKnn_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .queryImage(IMAGE_TEXT)
                .queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER)
                .modelId(MODEL_ID)
                .k(K)
                .expandNested(Boolean.TRUE)
                .boost(BOOST)
                .filter(TEST_FILTER)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a semantic field"
            + " using a dense model. query_tokens is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithSearchAnalyzerForKnn_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .queryImage(IMAGE_TEXT)
                .searchAnalyzer(TEST_SEMANTIC_FIELD_SEARCH_ANALYZER)
                .modelId(MODEL_ID)
                .k(K)
                .expandNested(Boolean.TRUE)
                .boost(BOOST)
                .filter(TEST_FILTER)
                .vectorSupplier(TEST_VECTOR_SUPPLIER)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a semantic field"
            + " using a dense model. semantic_field_search_analyzer is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithBothSearchAnalyzerAndModelIdForSparse_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .searchAnalyzer(TEST_SEMANTIC_FIELD_SEARCH_ANALYZER)
                .boost(BOOST)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: query_tokens, model_id and "
            + "semantic_field_search_analyzer can not coexist";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithEmptySearchAnalyzerForSparse_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                .queryText(QUERY_TEXT)
                .searchAnalyzer("")
                .boost(BOOST)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: " + "semantic_field_search_analyzer field can not be empty";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithEmptyModelIdForSparse_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                .queryText(QUERY_TEXT)
                .modelId("")
                .boost(BOOST)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: " + "model_id field can not be empty";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithModelIdAndRawTokensForSparse_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .embeddingFieldType(RankFeaturesFieldMapper.CONTENT_TYPE)
                .queryText(QUERY_TEXT)
                .queryTokensMapSupplier(TEST_QUERY_TOKENS_MAP_SUPPLIER)
                .modelId(MODEL_ID)
                .boost(BOOST)
                .queryName(QUERY_NAME)
                .isSemanticField(true)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage =
            "Failed to build the NeuralQueryBuilder: query_tokens, model_id and semantic_field_search_analyzer can not coexist";
        assertEquals(expectedMessage, exception.getMessage());
    }

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithSearchAnalyzerFieldForKnn_thenFail() {
        setUpClusterService();

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> NeuralQueryBuilder.builder()
                .fieldName(FIELD_NAME)
                .queryText(QUERY_TEXT)
                .modelId(MODEL_ID)
                .searchAnalyzer(TEST_SEMANTIC_FIELD_SEARCH_ANALYZER)
                .buildStage(NeuralQueryBuildStage.REWRITE)
                .isSemanticField(true)
                .embeddingFieldType(KNNVectorFieldMapper.CONTENT_TYPE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a semantic field"
            + " using a dense model. semantic_field_search_analyzer is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

}
