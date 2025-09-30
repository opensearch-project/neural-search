/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.mmr;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.search.processor.mmr.MMRRerankContext;
import org.opensearch.knn.search.processor.mmr.MMRTransformContext;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.util.TestUtils;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.MappingConstants.TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;

public class MMRNeuralQueryTransformerTests extends OpenSearchQueryTestCase {
    private final MMRNeuralQueryTransformer transformer = new MMRNeuralQueryTransformer();

    @Before
    public void setup() {
        // Initialize EventStatsManager for tests
        TestUtils.initializeEventStatsManager();
    }

    public void testTransform_setsKWhenNoMaxDistanceOrMinScore() {
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, "field");
        MMRTransformContext context = mockTransformContext(5, true, null);

        transformer.transform(queryBuilder, createSuccessListener(() -> verify(queryBuilder).k(5)), context);
    }

    public void testTransform_notSetsKWhenMaxDistanceExists() {
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(1f, null, "field");
        MMRTransformContext context = mockTransformContext(5, true, null);

        transformer.transform(queryBuilder, createSuccessListener(() -> verify(queryBuilder, never()).k(5)), context);
    }

    public void testTransform_notSetsKWhenMinScoreExists() {
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, 1f, "field");
        MMRTransformContext context = mockTransformContext(5, true, null);

        transformer.transform(queryBuilder, createSuccessListener(() -> verify(queryBuilder, never()).k(5)), context);
    }

    public void testTransform_whenRemoteIndicesWithoutVectorPath_thenException() {
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, "field");
        MMRTransformContext context = mock(MMRTransformContext.class);
        when(context.getRemoteIndices()).thenReturn(List.of("remote:index"));

        assertListenerReceivesException(
            queryBuilder,
            context,
            IllegalArgumentException.class,
            "[vector_field_path] in the mmr search extension should be provided for remote indices [remote:index]."
        );
    }

    public void testTransform_whenNullQueryFieldName_thenException() {
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, null);
        MMRTransformContext context = mock(MMRTransformContext.class);

        assertListenerReceivesException(
            queryBuilder,
            context,
            IllegalArgumentException.class,
            "Failed to transform the neural query for MMR. Query field name should not be null."
        );
    }

    public void testTransform_whenQueryFieldKnn_thenSuccess() {
        String fieldName = "vector_field";
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, fieldName);
        MMRRerankContext rerankContext = mock(MMRRerankContext.class);
        MMRTransformContext context = mockTransformContext(5, false, rerankContext);

        IndexMetadata indexMetadata = mockIndexMetadata(
            "test_index",
            Map.of(PROPERTIES, Map.of(fieldName, Map.of(TYPE, KNNVectorFieldMapper.CONTENT_TYPE)))
        );
        when(context.getLocalIndexMetadataList()).thenReturn(List.of(indexMetadata));

        transformer.transform(queryBuilder, createSuccessListener(() -> {
            verify(queryBuilder).k(5);
            verify(rerankContext).setVectorFieldPath(fieldName);
            verify(rerankContext).setSpaceType(SpaceType.L2);
            verify(rerankContext).setVectorDataType(VectorDataType.FLOAT);
            verifyNoMoreInteractions(rerankContext);
        }), context);
    }

    public void testTransform_whenQueryFieldSemanticAndMultipleIndices_thenSuccess() {
        String fieldName = "semantic_field";
        String semanticFieldInfoName = "semantic_field_semantic_info";
        String semanticFieldInfoName1 = "semantic_field_semantic_info1";

        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, fieldName);
        MMRRerankContext rerankContext = mock(MMRRerankContext.class);
        MMRTransformContext context = mockTransformContext(5, false, rerankContext);

        IndexMetadata idx1 = mockIndexMetadata(
            "test_index",
            Map.of(
                PROPERTIES,
                Map.of(
                    fieldName,
                    Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE),
                    semanticFieldInfoName,
                    Map.of(PROPERTIES, Map.of(EMBEDDING_FIELD_NAME, Map.of(TYPE, KNNVectorFieldMapper.CONTENT_TYPE)))
                )
            )
        );

        IndexMetadata idx2 = mockIndexMetadata(
            "test_index1",
            Map.of(
                PROPERTIES,
                Map.of(
                    fieldName,
                    Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE, SEMANTIC_INFO_FIELD_NAME, semanticFieldInfoName1),
                    semanticFieldInfoName1,
                    Map.of(PROPERTIES, Map.of(EMBEDDING_FIELD_NAME, Map.of(TYPE, KNNVectorFieldMapper.CONTENT_TYPE)))
                )
            )
        );

        when(context.getLocalIndexMetadataList()).thenReturn(List.of(idx1, idx2));

        transformer.transform(queryBuilder, createSuccessListener(() -> {
            verify(queryBuilder).k(5);
            verify(rerankContext).setIndexToVectorFieldPathMap(
                Map.of("test_index", "semantic_field_semantic_info.embedding", "test_index1", "semantic_field_semantic_info1.embedding")
            );
            verify(rerankContext).setSpaceType(SpaceType.L2);
            verify(rerankContext).setVectorDataType(VectorDataType.FLOAT);
            verifyNoMoreInteractions(rerankContext);
        }), context);
    }

    public void testTransform_whenQueryFieldSemanticAndChunkingEnabled_thenException() {
        String fieldName = "semantic_field";
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, fieldName);
        MMRTransformContext context = mockTransformContext(5, false, mock(MMRRerankContext.class));

        IndexMetadata indexMetadata = mockIndexMetadata(
            "test_index",
            Map.of(
                PROPERTIES,
                Map.of(
                    fieldName,
                    Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE, CHUNKING, Boolean.TRUE),
                    "semantic_field_semantic_info",
                    Map.of(PROPERTIES, Map.of(EMBEDDING_FIELD_NAME, Map.of(TYPE, KNNVectorFieldMapper.CONTENT_TYPE)))
                )
            )
        );
        when(context.getLocalIndexMetadataList()).thenReturn(List.of(indexMetadata));

        assertListenerReceivesException(
            queryBuilder,
            context,
            IllegalArgumentException.class,
            "Field [semantic_field] is a semantic field with chunking enabled, which can produce multiple vectors per document. MMR reranking does not support multiple vectors per document."
        );
    }

    public void testTransform_whenQueryFieldSemanticAndNotVectorField_thenException() {
        String fieldName = "semantic_field";
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, fieldName);
        MMRTransformContext context = mockTransformContext(5, false, mock(MMRRerankContext.class));

        IndexMetadata indexMetadata = mockIndexMetadata(
            "test_index",
            Map.of(PROPERTIES, Map.of(fieldName, Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE)))
        );
        when(context.getLocalIndexMetadataList()).thenReturn(List.of(indexMetadata));

        assertListenerReceivesException(
            queryBuilder,
            context,
            IllegalStateException.class,
            "Failed to find the vector field [semantic_field_semantic_info.embedding] from index mapping for the semantic field [semantic_field] when transform the neural query for MMR."
        );
    }

    public void testTransform_whenQueryFieldSemanticAndNotKnnVector_thenException() {
        String fieldName = "semantic_field";
        NeuralQueryBuilder queryBuilder = mockNeuralQueryBuilder(null, null, fieldName);
        MMRTransformContext context = mockTransformContext(5, false, mock(MMRRerankContext.class));

        IndexMetadata indexMetadata = mockIndexMetadata(
            "test_index",
            Map.of(
                PROPERTIES,
                Map.of(
                    fieldName,
                    Map.of(TYPE, SemanticFieldMapper.CONTENT_TYPE),
                    "semantic_field_semantic_info",
                    Map.of(PROPERTIES, Map.of(EMBEDDING_FIELD_NAME, Map.of(TYPE, RankFeaturesFieldMapper.CONTENT_TYPE)))
                )
            )
        );
        when(context.getLocalIndexMetadataList()).thenReturn(List.of(indexMetadata));

        assertListenerReceivesException(
            queryBuilder,
            context,
            IllegalArgumentException.class,
            "Field [semantic_field] is a semantic field with a non-KNN embedding [rank_features]. MMR reranking only can support knn_vector field."
        );
    }

    private NeuralQueryBuilder mockNeuralQueryBuilder(Float maxDistance, Float minScore, String fieldName) {
        NeuralQueryBuilder builder = mock(NeuralQueryBuilder.class);
        when(builder.maxDistance()).thenReturn(maxDistance);
        when(builder.minScore()).thenReturn(minScore);
        when(builder.fieldName()).thenReturn(fieldName);
        return builder;
    }

    private MMRTransformContext mockTransformContext(int candidates, boolean resolved, MMRRerankContext rerankContext) {
        MMRTransformContext context = mock(MMRTransformContext.class);
        when(context.getCandidates()).thenReturn(candidates);
        when(context.isVectorFieldInfoResolved()).thenReturn(resolved);
        if (rerankContext != null) {
            when(context.getMmrRerankContext()).thenReturn(rerankContext);
        }
        return context;
    }

    private IndexMetadata mockIndexMetadata(String indexName, Map<String, Object> sourceMap) {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index(indexName, "uuid"));
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(mappingMetadata.sourceAsMap()).thenReturn(sourceMap);
        return indexMetadata;
    }

    private ActionListener<Void> createSuccessListener(Runnable verification) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                verification.run();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        };
    }

    private void assertListenerReceivesException(
        NeuralQueryBuilder queryBuilder,
        MMRTransformContext context,
        Class<? extends Exception> expectedType,
        String expectedMessage
    ) {
        @SuppressWarnings("unchecked")
        ActionListener<Void> listener = mock(ActionListener.class);
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);

        transformer.transform(queryBuilder, listener, context);

        verify(listener).onFailure(captor.capture());
        Exception e = captor.getValue();
        assertNotNull("Expected exception to be passed to listener", e);
        assertTrue("Expected " + expectedType.getSimpleName(), expectedType.isInstance(e));
        assertEquals(expectedMessage, e.getMessage());
    }
}
