/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.mapper.dto.ModelSelection;
import org.opensearch.neuralsearch.settings.SemanticModelSelectionSettingsAccessor;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.when;

public class DefaultSemanticModelResolverTests extends OpenSearchTestCase {

    @Mock
    private SemanticModelSelectionSettingsAccessor settingsAccessor;

    private DefaultSemanticModelResolver resolver;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        resolver = new DefaultSemanticModelResolver(settingsAccessor);
    }

    public void testResolve_whenNoModelConfigured_thenThrow() {
        when(settingsAccessor.getModelId("sparse.english")).thenReturn(null);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> resolver.resolve(new ModelSelection("ENGLISH", "SPARSE"))
        );
        assertTrue(e.getMessage().contains("No model_id is configured"));
        assertTrue(e.getMessage().contains("plugins.neural_search.model_selection.model_id.sparse.english"));
    }

    public void testResolve_whenSparseModelConfigured_thenReturnModelId() {
        when(settingsAccessor.getModelId("sparse.english")).thenReturn("sparse_model_id");

        assertEquals("sparse_model_id", resolver.resolve(new ModelSelection("ENGLISH", "SPARSE")));
    }

    public void testResolve_whenDenseModelConfigured_thenReturnModelId() {
        when(settingsAccessor.getModelId("dense.multilingual")).thenReturn("dense_model_id");

        assertEquals("dense_model_id", resolver.resolve(new ModelSelection("MULTILINGUAL", "DENSE")));
    }
}
