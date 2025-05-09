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

    public void testBuilderInstantiation_whenBuiltDuringRewriteWithRequiredFieldsForKnn_thenBuildSuccessfully() {
        setUpClusterService();

        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .queryText(QUERY_TEXT)
            .modelId(MODEL_ID)
            .builStage(NeuralQueryBuildStage.REWRITE)
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
            .builStage(NeuralQueryBuildStage.REWRITE)
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
            .builStage(NeuralQueryBuildStage.REWRITE)
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
            .builStage(NeuralQueryBuildStage.REWRITE)
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
            .modelId(MODEL_ID)
            .boost(BOOST)
            .queryName(QUERY_NAME)
            .isSemanticField(true)
            .builStage(NeuralQueryBuildStage.REWRITE)
            .build();

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(TEST_QUERY_TOKENS_MAP_SUPPLIER, neuralQueryBuilder.queryTokensMapSupplier());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
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
                .builStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a semantic field "
            + "using a sparse model. [filter, query_image, k, expand_nested_docs] are not supported since they "
            + "are for the dense model.";
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
                .builStage(NeuralQueryBuildStage.REWRITE)
                .build()
        );

        final String expectedMessage = "Failed to build the NeuralQueryBuilder: Target field is a semantic field"
            + " using a dense model. query_tokens is not supported since it is for the sparse model.";
        assertEquals(expectedMessage, exception.getMessage());
    }

}
