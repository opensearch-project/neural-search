/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.RemoteModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.test.OpenSearchTestCase;

public class AsymmetricModelDetectorTests extends OpenSearchTestCase {

    public void testIsAsymmetricModel_withNullModel() {
        assertFalse(AsymmetricModelDetector.isAsymmetricModel(null));
    }

    public void testIsAsymmetricModel_withNullModelConfig() {
        MLModel model = mock(MLModel.class);
        when(model.getModelConfig()).thenReturn(null);

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_withUnknownModelConfig() {
        MLModel model = mock(MLModel.class);
        MLModelConfig config = mock(MLModelConfig.class);
        when(model.getModelConfig()).thenReturn(config);

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    // Remote model tests
    public void testIsAsymmetricModel_remoteModel_withExplicitAsymmetricTrue() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", true));

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withExplicitAsymmetricFalse() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("is_asymmetric", false));

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withQueryPrefix() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("query_prefix", "query: "));

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withPassagePrefix() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("passage_prefix", "passage: "));

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withBothPrefixes() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("query_prefix", "query: ", "passage_prefix", "passage: "));

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withNoAsymmetricConfig() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(Map.of("other_config", "value"));

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_remoteModel_withNullAdditionalConfig() {
        MLModel model = mock(MLModel.class);
        RemoteModelConfig config = mock(RemoteModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getAdditionalConfig()).thenReturn(null);

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    // Local model tests
    public void testIsAsymmetricModel_localModel_withQueryPrefix() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn("query: ");
        when(config.getPassagePrefix()).thenReturn(null);

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_localModel_withPassagePrefix() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn(null);
        when(config.getPassagePrefix()).thenReturn("passage: ");

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_localModel_withBothPrefixes() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn("query: ");
        when(config.getPassagePrefix()).thenReturn("passage: ");

        assertTrue(AsymmetricModelDetector.isAsymmetricModel(model));
    }

    public void testIsAsymmetricModel_localModel_withNoPrefixes() {
        MLModel model = mock(MLModel.class);
        TextEmbeddingModelConfig config = mock(TextEmbeddingModelConfig.class);

        when(model.getModelConfig()).thenReturn(config);
        when(config.getQueryPrefix()).thenReturn(null);
        when(config.getPassagePrefix()).thenReturn(null);

        assertFalse(AsymmetricModelDetector.isAsymmetricModel(model));
    }
}
