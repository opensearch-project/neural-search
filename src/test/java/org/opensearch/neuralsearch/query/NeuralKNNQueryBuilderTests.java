/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.Query;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.mapper.KNNMappingConfig;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.core.index.Index;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.Version;
import org.opensearch.knn.index.query.rescore.RescoreContext;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NeuralKNNQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "test_field";
    private static final float[] VECTOR = new float[] { 1.0f, 2.0f, 3.0f };
    private static final int K = 10;
    private static final Map<String, Object> METHOD_PARAMETERS = Map.of("ef_search", 100);

    private QueryShardContext mockContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockContext = mock(QueryShardContext.class);

        // Mock KNN field type
        KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldType.class);
        KNNMappingConfig mockKNNMappingConfig = mock(KNNMappingConfig.class);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, MethodComponentContext.EMPTY);

        when(mockKNNVectorField.getKnnMappingConfig()).thenReturn(mockKNNMappingConfig);
        when(mockKNNMappingConfig.getKnnMethodContext()).thenReturn(Optional.of(knnMethodContext));
        when(mockKNNVectorField.getKnnMappingConfig().getDimension()).thenReturn(3);
        when(mockKNNVectorField.getVectorDataType()).thenReturn(VectorDataType.FLOAT);
        when(mockKNNVectorField.typeName()).thenReturn("knn_vector");
        when(mockKNNVectorField.name()).thenReturn(FIELD_NAME);

        // Mock query shard context
        Index dummyIndex = new Index("dummy", "dummy");
        when(mockContext.index()).thenReturn(dummyIndex);
        when(mockContext.fieldMapper(eq(FIELD_NAME))).thenReturn(mockKNNVectorField);

        // Mock index settings
        IndexMetadata indexMetadata = IndexMetadata.builder("dummy")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockContext.getIndexSettings()).thenReturn(indexSettings);
    }

    public void testBuilder_withRequiredFields() {
        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Field name should match", "neural_knn", queryBuilder.getWriteableName());
    }

    public void testBuilder_withAllFields() {
        QueryBuilder filter = mock(QueryBuilder.class);

        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .k(K)
            .filter(filter)
            .expandNested(true)
            .methodParameters(METHOD_PARAMETERS)
            .build();

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Field name should match", "neural_knn", queryBuilder.getWriteableName());
    }

    public void testDoToQuery() throws IOException {
        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        Query query = queryBuilder.doToQuery(mockContext);
        assertNotNull("Query should not be null", query);
        assertTrue("Query should be instance of NeuralKNNQuery", query instanceof NeuralKNNQuery);
    }

    public void testDoRewrite() throws IOException {
        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        QueryBuilder rewritten = queryBuilder.doRewrite(mockContext);
        assertNotNull("Rewritten query should not be null", rewritten);
        assertTrue("Rewritten query should be instance of NeuralKNNQueryBuilder", rewritten instanceof NeuralKNNQueryBuilder);
    }

    public void testEquals() {
        NeuralKNNQueryBuilder builder1 = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        NeuralKNNQueryBuilder builder2 = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        assertEquals("Identical builders should be equal", builder1, builder2);
        assertEquals("Identical builders should have same hash code", builder1.hashCode(), builder2.hashCode());
    }

    public void testNotEquals() {
        NeuralKNNQueryBuilder builder1 = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        NeuralKNNQueryBuilder builder2 = NeuralKNNQueryBuilder.builder().fieldName("different_field").vector(VECTOR).k(K).build();

        assertNotEquals("Different builders should not be equal", builder1, builder2);
        assertNotEquals("Different builders should have different hash codes", builder1.hashCode(), builder2.hashCode());
    }

    public void testBuilder_withOriginalQueryText() {
        String originalQueryText = "test query text";
        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .k(K)
            .originalQueryText(originalQueryText)
            .build();

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Original query text should match", originalQueryText, queryBuilder.getOriginalQueryText());
    }

    public void testBuilder_withMethodParameters() {
        Map<String, Object> methodParams = Map.of("ef_search", 100);
        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .k(K)
            .methodParameters(methodParams)
            .build();

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Method parameters should match", methodParams, queryBuilder.getKnnQueryBuilder().getMethodParameters());
    }

    public void testBuilder_withAllFieldsAndOriginalQueryText() {
        String originalQueryText = "test query text";
        Map<String, Object> methodParams = Map.of("ef_search", 100);
        RescoreContext rescoreContext = RescoreContext.getDefault();
        QueryBuilder filter = mock(QueryBuilder.class);

        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .k(K)
            .originalQueryText(originalQueryText)
            .methodParameters(methodParams)
            .rescoreContext(rescoreContext)
            .filter(filter)
            .expandNested(true)
            .build();

        assertNotNull("Query builder should not be null", queryBuilder);
        assertEquals("Original query text should match", originalQueryText, queryBuilder.getOriginalQueryText());
        assertEquals("Method parameters should match", methodParams, queryBuilder.getKnnQueryBuilder().getMethodParameters());
        assertEquals("Rescore context should match", rescoreContext, queryBuilder.getKnnQueryBuilder().getRescoreContext());
        assertEquals("Filter should match", filter, queryBuilder.getKnnQueryBuilder().getFilter());
        assertTrue("Expand nested should be true", queryBuilder.getKnnQueryBuilder().getExpandNested());
    }

    public void testDoToQuery_withMultiNodeSettings() throws IOException {
        // Mock index settings with multiple shards to simulate multi-node scenario
        IndexMetadata indexMetadata = IndexMetadata.builder("dummy")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(3)
            .numberOfReplicas(1)
            .build();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(3))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, Integer.toString(1))
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockContext.getIndexSettings()).thenReturn(indexSettings);

        NeuralKNNQueryBuilder queryBuilder = NeuralKNNQueryBuilder.builder().fieldName(FIELD_NAME).vector(VECTOR).k(K).build();

        Query query = queryBuilder.doToQuery(mockContext);
        assertNotNull("Query should not be null", query);
        assertTrue("Query should be instance of NeuralKNNQuery", query instanceof NeuralKNNQuery);
    }

    public void testSerialization_withMinScoreParameter() throws IOException {
        // Test serialization/deserialization with min_score (radial search parameter)
        String originalQueryText = "test query for radial search";
        Float minScore = 0.7f;

        NeuralKNNQueryBuilder originalBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .minScore(minScore)
            .originalQueryText(originalQueryText)
            .build();

        // Serialize to StreamOutput
        org.opensearch.common.io.stream.BytesStreamOutput streamOutput = new org.opensearch.common.io.stream.BytesStreamOutput();
        streamOutput.setVersion(org.opensearch.Version.CURRENT);
        originalBuilder.writeTo(streamOutput);

        // Deserialize from StreamInput
        org.opensearch.core.common.io.stream.StreamInput streamInput = streamOutput.bytes().streamInput();
        streamInput.setVersion(org.opensearch.Version.CURRENT);
        NeuralKNNQueryBuilder deserializedBuilder = new NeuralKNNQueryBuilder(streamInput);

        // Verify the deserialized builder matches the original
        assertEquals(
            "Original query text should match",
            originalBuilder.getOriginalQueryText(),
            deserializedBuilder.getOriginalQueryText()
        );
        assertEquals(
            "Min score should match",
            originalBuilder.getKnnQueryBuilder().getMinScore(),
            deserializedBuilder.getKnnQueryBuilder().getMinScore()
        );

        // Verify builders are equal (this checks all internal fields including field name and vector)
        assertEquals("Serialized and deserialized builders should be equal", originalBuilder, deserializedBuilder);
        assertEquals("Hash codes should match", originalBuilder.hashCode(), deserializedBuilder.hashCode());
    }

    public void testSerialization_withMaxDistanceParameter() throws IOException {
        // Test serialization/deserialization with max_distance (radial search parameter)
        String originalQueryText = "test query for distance-based search";
        Float maxDistance = 0.5f;

        NeuralKNNQueryBuilder originalBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .maxDistance(maxDistance)
            .originalQueryText(originalQueryText)
            .build();

        // Serialize to StreamOutput
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        originalBuilder.writeTo(streamOutput);

        // Deserialize from StreamInput
        StreamInput streamInput = streamOutput.bytes().streamInput();
        streamInput.setVersion(Version.CURRENT);
        NeuralKNNQueryBuilder deserializedBuilder = new NeuralKNNQueryBuilder(streamInput);

        // Verify the deserialized builder matches the original
        assertEquals(
            "Original query text should match",
            originalBuilder.getOriginalQueryText(),
            deserializedBuilder.getOriginalQueryText()
        );
        assertEquals(
            "Max distance should match",
            originalBuilder.getKnnQueryBuilder().getMaxDistance(),
            deserializedBuilder.getKnnQueryBuilder().getMaxDistance()
        );

        // Verify builders are equal (this checks all internal fields including field name and vector)
        assertEquals("Serialized and deserialized builders should be equal", originalBuilder, deserializedBuilder);
        assertEquals("Hash codes should match", originalBuilder.hashCode(), deserializedBuilder.hashCode());
    }

    public void testSerialization_withKParameter() throws IOException {
        // Test serialization/deserialization with standard k parameter (non-radial search)
        String originalQueryText = "test query for standard k-NN search";
        Integer k = 5;

        NeuralKNNQueryBuilder originalBuilder = NeuralKNNQueryBuilder.builder()
            .fieldName(FIELD_NAME)
            .vector(VECTOR)
            .k(k)
            .originalQueryText(originalQueryText)
            .build();

        // Serialize to StreamOutput
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        originalBuilder.writeTo(streamOutput);

        // Deserialize from StreamInput
        StreamInput streamInput = streamOutput.bytes().streamInput();
        streamInput.setVersion(Version.CURRENT);
        NeuralKNNQueryBuilder deserializedBuilder = new NeuralKNNQueryBuilder(streamInput);

        // Verify the deserialized builder matches the original
        assertEquals(
            "Original query text should match",
            originalBuilder.getOriginalQueryText(),
            deserializedBuilder.getOriginalQueryText()
        );
        assertEquals("K should match", originalBuilder.getKnnQueryBuilder().getK(), deserializedBuilder.getKnnQueryBuilder().getK());

        // Verify builders are equal (this checks all internal fields including field name and vector)
        assertEquals("Serialized and deserialized builders should be equal", originalBuilder, deserializedBuilder);
        assertEquals("Hash codes should match", originalBuilder.hashCode(), deserializedBuilder.hashCode());
    }
}
