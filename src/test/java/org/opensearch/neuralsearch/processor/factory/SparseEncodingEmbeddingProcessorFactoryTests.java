/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.SparseEncodingProcessor.TYPE;
import static org.opensearch.neuralsearch.util.prune.PruneUtils.PRUNE_TYPE_FIELD;
import static org.opensearch.neuralsearch.util.prune.PruneUtils.PRUNE_RATIO_FIELD;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class SparseEncodingEmbeddingProcessorFactoryTests extends OpenSearchTestCase {
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String MODEL_ID = "testModelId";
    private static final int BATCH_SIZE = 1;

    private MLCommonsClientAccessor clientAccessor;
    private Environment environment;
    private ClusterService clusterService;
    private SparseEncodingProcessorFactory sparseEncodingProcessorFactory;

    @Before
    public void setup() {
        clientAccessor = mock(MLCommonsClientAccessor.class);
        environment = mock(Environment.class);
        clusterService = mock(ClusterService.class);
        sparseEncodingProcessorFactory = new SparseEncodingProcessorFactory(clientAccessor, environment, clusterService);
    }

    @SneakyThrows
    public void testCreateProcessor_whenAllRequiredParamsPassed_thenSuccessful() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));

        SparseEncodingProcessor processor = (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(
            Map.of(),
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        assertNotNull(processor);
        assertEquals(TYPE, processor.getType());
        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertEquals(DESCRIPTION, processor.getDescription());
        assertEquals(PruneType.NONE, processor.getPruneType());
        assertEquals(0f, processor.getPruneRatio(), 1e-6);
    }

    @SneakyThrows
    public void testCreateProcessor_whenPruneParamsPassed_thenSuccessful() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        config.put(PRUNE_TYPE_FIELD, "top_k");
        config.put(PRUNE_RATIO_FIELD, 2f);

        SparseEncodingProcessor processor = (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(
            Map.of(),
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        assertNotNull(processor);
        assertEquals(TYPE, processor.getType());
        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertEquals(DESCRIPTION, processor.getDescription());
        assertEquals(PruneType.TOP_K, processor.getPruneType());
        assertEquals(2f, processor.getPruneRatio(), 1e-6);
    }

    @SneakyThrows
    public void testCreateProcessor_whenEmptyFieldMapField_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("Unable to create the processor as field_map has invalid key or value", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenMissingModelIdField_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        OpenSearchParseException exception = assertThrows(
            OpenSearchParseException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[model_id] required property is missing", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenMissingFieldMapField_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        OpenSearchParseException exception = assertThrows(
            OpenSearchParseException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[field_map] required property is missing", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenInvalidPruneType_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        config.put(PRUNE_TYPE_FIELD, "invalid_prune_type");
        config.put(PRUNE_RATIO_FIELD, 2f);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("Unknown prune type: invalid_prune_type", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenInvalidPruneRatio_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        config.put(PRUNE_TYPE_FIELD, "top_k");
        config.put(PRUNE_RATIO_FIELD, 0.2f);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("Illegal prune_ratio 0.200000 for prune_type: top_k. prune_ratio should be positive integer.", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenMissingPruneRatio_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        config.put(PRUNE_TYPE_FIELD, "alpha_mass");

        OpenSearchParseException exception = assertThrows(
            OpenSearchParseException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[prune_ratio] required property is missing", exception.getMessage());
    }

    @SneakyThrows
    public void testCreateProcessor_whenMissingPruneType_thenFail() {
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, MODEL_ID);
        config.put(FIELD_MAP_FIELD, Map.of("a", "b"));
        config.put(PRUNE_RATIO_FIELD, 0.1);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> sparseEncodingProcessorFactory.create(Map.of(), PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("prune_ratio field is not supported when prune_type is not provided", exception.getMessage());
    }
}
