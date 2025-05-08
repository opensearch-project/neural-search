/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.SemanticMLModelUtils.getModelType;

public class SemanticMLModelUtilsTests extends OpenSearchTestCase {
    public void testGetModelType_whenInvalidAlgorithm_thenException() {
        final MLModel mlModel = mock(MLModel.class);
        when(mlModel.getAlgorithm()).thenReturn(FunctionName.QUESTION_ANSWERING);
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> getModelType(mlModel));
        final String expectedMessage =
            "Semantic field cannot support the function name QUESTION_ANSWERING with the model id null. It should be one of ";
        assertTrue(exception.getMessage(), exception.getMessage().contains(expectedMessage));
    }

    public void testGetModelType_whenInvalidRemoteModelType_thenException() {
        final MLModel mlModel = mock(MLModel.class);
        final MLModelConfig mlModelConfig = mock(MLModelConfig.class);
        when(mlModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(mlModel.getModelConfig()).thenReturn(mlModelConfig);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> getModelType(mlModel));
        final String expectedMessage =
            "Semantic field cannot support the remote model type null with the model id null. It should be one of";
        assertTrue(exception.getMessage(), exception.getMessage().contains(expectedMessage));
    }

    public void testGetModelType_whenNoRemoteModelConfig_thenException() {
        final MLModel mlModel = mock(MLModel.class);
        when(mlModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> getModelType(mlModel));
        final String expectedMessage = "Model config is required for the remote model null used for semantic fields.";
        assertTrue(exception.getMessage(), exception.getMessage().contains(expectedMessage));
    }

    public void testGetModelType_whenValidSparseModel_thenException() {
        final MLModel mlModel = mock(MLModel.class);
        when(mlModel.getAlgorithm()).thenReturn(FunctionName.SPARSE_ENCODING);

        final String modelType = getModelType(mlModel);

        assertEquals(FunctionName.SPARSE_ENCODING.name(), modelType);
    }

    public void testGetModelType_whenValidRemoteTextEmbedding_thenException() {
        final MLModel mlModel = mock(MLModel.class);
        final MLModelConfig mlModelConfig = mock(MLModelConfig.class);
        when(mlModel.getAlgorithm()).thenReturn(FunctionName.REMOTE);
        when(mlModel.getModelConfig()).thenReturn(mlModelConfig);
        when(mlModelConfig.getModelType()).thenReturn(FunctionName.TEXT_EMBEDDING.name());

        final String modelType = getModelType(mlModel);

        assertEquals(FunctionName.REMOTE.name() + "_" + FunctionName.TEXT_EMBEDDING.name(), modelType);
    }
}
