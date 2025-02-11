/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.Query;
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
        assertEquals("Field name should match", "knn", queryBuilder.getWriteableName());
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
        assertEquals("Field name should match", "knn", queryBuilder.getWriteableName());
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
}
