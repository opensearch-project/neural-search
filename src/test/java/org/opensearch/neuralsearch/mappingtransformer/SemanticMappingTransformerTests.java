/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import lombok.NonNull;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.QuestionAnsweringModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.neuralsearch.constants.MappingConstants;
import org.opensearch.neuralsearch.constants.SemanticFieldConstants;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;

public class SemanticMappingTransformerTests extends OpenSearchTestCase {
    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    private NamedXContentRegistry namedXContentRegistry;

    @Mock
    private ActionListener<Void> listener;

    private SemanticMappingTransformer transformer;
    private final ClassLoader classLoader = this.getClass().getClassLoader();

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
        transformer = new SemanticMappingTransformer(mlClientAccessor, namedXContentRegistry);
    }

    public void testTransform_whenMappingNull_thenDoNothing() {
        transformer.transform(null, null, listener);

        verify(listener).onResponse(null);
    }

    public void testTransform_whenUnexpectedMappingFormat_thenDoNothing() {
        final Map<String, Object> unexpectedMapping = new HashMap<>();
        unexpectedMapping.put("properties", "value");
        transformer.transform(unexpectedMapping, null, listener);

        verify(listener).onResponse(null);
    }

    public void testTransform_whenNoSemanticField_thenDoNothing() {
        final Map<String, Object> unexpectedMapping = new HashMap<>();
        unexpectedMapping.put("properties", Map.of("field", Map.of("type", "text")));

        doAnswer(invocationOnMock -> {
            final Set<String> modelIds = invocationOnMock.getArgument(0);
            assert modelIds.isEmpty();
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Collections.emptyMap());
            return null;
        }).when(mlClientAccessor).getModels(any(), any(), any());

        transformer.transform(unexpectedMapping, null, listener);

        verify(listener).onResponse(null);
    }

    public void testTransform_whenSemanticFields_thenTransformMappingsSuccessfully() throws URISyntaxException, IOException {
        // prepare original mappings
        final String textEmbeddingModelId = "textEmbeddingModelId";
        final String sparseModelId = "sparseModelId";
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled1 = new HashMap<>();
        properties.put("inter_field.semantic_field_1", semanticFiled1);
        semanticFiled1.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled1.put(SemanticFieldConstants.MODEL_ID, textEmbeddingModelId);

        final String customSemanticInfoFieldName = "custom_semantic_info_field";
        final Map<String, Object> semanticFiled2 = new HashMap<>();
        properties.put("semantic_field_2", semanticFiled2);
        semanticFiled2.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled2.put(SemanticFieldConstants.MODEL_ID, sparseModelId);
        semanticFiled2.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, customSemanticInfoFieldName);

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{\"space_type\":\"l2\"}";
        final TextEmbeddingModelConfig textEmbeddingModelConfig = TextEmbeddingModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType("modelType")
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel textEmbeddingModel = MLModel.builder()
            .algorithm(FunctionName.TEXT_EMBEDDING)
            .modelConfig(textEmbeddingModelConfig)
            .build();

        final MLModel sparseModel = MLModel.builder().algorithm(FunctionName.SPARSE_ENCODING).build();

        // mock
        doAnswer(invocationOnMock -> {
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Map.of(textEmbeddingModelId, textEmbeddingModel, sparseModelId, sparseModel));
            return null;
        }).when(mlClientAccessor).getModels(any(), any(), any());

        // call
        transformer.transform(mappings, null, listener);

        // verify
        final String expectedTransformedMappingString = Files.readString(
            Path.of(classLoader.getResource("mappingtransformer/transformedMappingMultipleSemanticFields.json").toURI())
        );

        final Map<String, Object> expectedTransformedMapping = MapperService.parseMapping(
            namedXContentRegistry,
            expectedTransformedMappingString
        );
        assertEquals(expectedTransformedMapping, mappings);
    }

    public void testTransform_whenSemanticFieldsWithRemoteModels_thenTransformMappingsSuccessfully() throws URISyntaxException,
        IOException {
        // prepare original mappings
        final String remoteTextEmbeddingModelId = "textEmbeddingModelId";
        final String remoteSparseModelId = "sparseModelId";
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled1 = new HashMap<>();
        properties.put("inter_field.semantic_field_1", semanticFiled1);
        semanticFiled1.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled1.put(SemanticFieldConstants.MODEL_ID, remoteTextEmbeddingModelId);

        final String customSemanticInfoFieldName = "custom_semantic_info_field";
        final Map<String, Object> semanticFiled2 = new HashMap<>();
        properties.put("semantic_field_2", semanticFiled2);
        semanticFiled2.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled2.put(SemanticFieldConstants.MODEL_ID, remoteSparseModelId);
        semanticFiled2.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, customSemanticInfoFieldName);

        // prepare mock model config
        final Integer embeddingDimension = 768;
        final String allConfig = "{\"space_type\":\"l2\"}";
        final TextEmbeddingModelConfig remoteTextEmbeddingModelConfig = TextEmbeddingModelConfig.builder()
            .embeddingDimension(embeddingDimension)
            .allConfig(allConfig)
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel remoteTextEmbeddingModel = MLModel.builder()
            .algorithm(FunctionName.REMOTE)
            .modelConfig(remoteTextEmbeddingModelConfig)
            .build();

        // Currently there is no dedicated model config for remote model and we are using TextEmbeddingModelConfig
        // in ml-common. Once ml-common created a dedicated one we should replace it.
        final MLModelConfig remoteSparseModelConfig = TextEmbeddingModelConfig.builder()
            .embeddingDimension(0) // This is required for TextEmbeddingModelConfig even we don't need it for the remote sparse model
            .modelType(FunctionName.SPARSE_TOKENIZE.name())
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel remoteSparseModel = MLModel.builder().algorithm(FunctionName.REMOTE).modelConfig(remoteSparseModelConfig).build();

        // mock
        doAnswer(invocationOnMock -> {
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Map.of(remoteTextEmbeddingModelId, remoteTextEmbeddingModel, remoteSparseModelId, remoteSparseModel));
            return null;
        }).when(mlClientAccessor).getModels(any(), any(), any());

        // call
        transformer.transform(mappings, null, listener);

        // verify
        final String expectedTransformedMappingString = Files.readString(
            Path.of(classLoader.getResource("mappingtransformer/transformedMappingMultipleSemanticFields.json").toURI())
        );

        final Map<String, Object> expectedTransformedMapping = MapperService.parseMapping(
            namedXContentRegistry,
            expectedTransformedMappingString
        );
        assertEquals(expectedTransformedMapping, mappings);
    }

    public void testTransform_whenWrongModelConfig_thenException() {
        // prepare original mappings
        final String dummyModelId = "dummyModelId";
        final Map<String, Object> mappings = getBaseMappingsWithOneSemanticField(dummyModelId);

        // prepare mock model config
        QuestionAnsweringModelConfig questionAnsweringModelConfig = QuestionAnsweringModelConfig.builder()
            .modelType(FunctionName.TEXT_EMBEDDING.name())
            .frameworkType(QuestionAnsweringModelConfig.FrameworkType.HUGGINGFACE_TRANSFORMERS)
            .build();
        final MLModel dummyModel = MLModel.builder()
            .modelId(dummyModelId)
            .algorithm(FunctionName.REMOTE)
            .modelConfig(questionAnsweringModelConfig)
            .build();

        // mock
        doAnswer(invocationOnMock -> {
            final Consumer<Map<String, MLModel>> onSuccess = invocationOnMock.getArgument(1);
            onSuccess.accept(Map.of(dummyModelId, dummyModel));
            return null;
        }).when(mlClientAccessor).getModels(any(), any(), any());

        // call
        transformer.transform(mappings, null, listener);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        // Then: Assert that the exception message is as expected
        final Exception capturedException = exceptionCaptor.getValue();
        final String expectedErrorMessage =
            "Failed to transform the mapping for the semantic field at semantic_field due to Model dummyModelId is a remote text embedding model but model config is not a text embedding config";
        assertEquals(expectedErrorMessage, capturedException.getMessage());
    }

    public void testTransform_whenMultipleModelsNotFound_thenException() {
        // prepare original mappings
        final String notFoundModel1 = "notFoundModel1";
        final String notFoundModel2 = "notFoundModel2";
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled = new HashMap<>();
        properties.put("semantic_field", semanticFiled);
        semanticFiled.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled.put(SemanticFieldConstants.MODEL_ID, notFoundModel1);

        final Map<String, Object> semanticFiled1 = new HashMap<>();
        properties.put("semantic_field1", semanticFiled1);
        semanticFiled1.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled1.put(SemanticFieldConstants.MODEL_ID, notFoundModel2);

        // mock
        doAnswer(invocationOnMock -> {
            final Consumer<Exception> onFailure = invocationOnMock.getArgument(2);
            onFailure.accept(new RuntimeException("Model notFoundModel1 is not found; Model notFoundModel2 is not found"));
            return null;
        }).when(mlClientAccessor).getModels(any(), any(), any());

        // call
        transformer.transform(mappings, null, listener);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        // Then: Assert that the exception message is as expected
        final Exception capturedException = exceptionCaptor.getValue();
        final String expectedErrorMessage = "Model notFoundModel1 is not found; Model notFoundModel2 is not found";
        assertEquals(expectedErrorMessage, capturedException.getMessage());
    }

    public void testTransform_whenInvalidCustomSemanticInfoFieldName_thenException() {
        // prepare original mappings
        final String dummyModelId = "dummyModelId";
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled = new HashMap<>();
        properties.put("semantic_field", semanticFiled);
        semanticFiled.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled.put(SemanticFieldConstants.MODEL_ID, dummyModelId);
        semanticFiled.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, new ArrayList<>());

        // call
        transformer.transform(mappings, null, listener);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        // Then: Assert that the exception message is as expected
        final Exception capturedException = exceptionCaptor.getValue();
        final String expectedErrorMessage =
            "semantic_info_field_name should be a non-empty string for the semantic field at semantic_field";
        assertEquals(expectedErrorMessage, capturedException.getMessage());
    }

    public void testTransform_whenMissingModelId_thenException() {
        // prepare original mappings
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled = new HashMap<>();
        properties.put("semantic_field", semanticFiled);
        semanticFiled.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);

        // call
        transformer.transform(mappings, null, listener);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        // Then: Assert that the exception message is as expected
        final Exception capturedException = exceptionCaptor.getValue();
        final String expectedErrorMessage = "model_id is required for the semantic field at semantic_field";
        assertEquals(expectedErrorMessage, capturedException.getMessage());
    }

    public void testTransform_whenMultipleValidationErrors_thenException() {
        // prepare original mappings
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled = new HashMap<>();
        properties.put("semantic_field", semanticFiled);
        semanticFiled.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled.put(SemanticFieldConstants.MODEL_ID, new ArrayList<>());
        semanticFiled.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, "dummy_name");

        final Map<String, Object> semanticFiled1 = new HashMap<>();
        properties.put("semantic_field1", semanticFiled1);
        semanticFiled1.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled1.put(SemanticFieldConstants.MODEL_ID, "");
        semanticFiled1.put(SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME, "invalid_name.with_dot");

        // call
        transformer.transform(mappings, null, listener);

        // Capture the exception passed to onFailure
        final ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        // Then: Assert that the exception message is as expected
        final Exception capturedException = exceptionCaptor.getValue();
        final String expectedErrorMessage = "model_id should be a non-empty string for the semantic field at "
            + "semantic_field1; semantic_info_field_name should not contain '.' for the semantic field at "
            + "semantic_field1; model_id should be a non-empty string for the semantic field at semantic_field";
        assertEquals(expectedErrorMessage, capturedException.getMessage());
    }

    private Map<String, Object> getBaseMappingsWithOneSemanticField(@NonNull final String modelId) {
        final Map<String, Object> mappings = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);
        final Map<String, Object> semanticFiled = new HashMap<>();
        properties.put("semantic_field", semanticFiled);
        semanticFiled.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
        semanticFiled.put(SemanticFieldConstants.MODEL_ID, modelId);
        return mappings;
    }
}
