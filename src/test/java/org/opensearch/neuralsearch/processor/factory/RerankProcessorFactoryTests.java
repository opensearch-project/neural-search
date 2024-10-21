/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.RERANKER_MAX_DOC_FIELDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.ByFieldRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.MLOpenSearchRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.neuralsearch.processor.rerank.context.DocumentContextSourceFetcher;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class RerankProcessorFactoryTests extends OpenSearchTestCase {

    final String TAG = "default-tag";
    final String DESC = "processor description";

    private RerankProcessorFactory factory;

    @Mock
    private MLCommonsClientAccessor clientAccessor;

    @Mock
    private PipelineContext pipelineContext;

    @Mock
    private ClusterService clusterService;

    @Before
    public void setup() {
        clusterService = mock(ClusterService.class);
        pipelineContext = mock(PipelineContext.class);
        clientAccessor = mock(MLCommonsClientAccessor.class);
        factory = new RerankProcessorFactory(clientAccessor, clusterService);
        doReturn(Settings.EMPTY).when(clusterService).getSettings();
    }

    public void testRerankProcessorFactory_whenEmptyConfig_thenFail() {
        Map<String, Object> config = new HashMap<>(Map.of());
        assertThrows(
            "no rerank type found",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_whenNonExistentType_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of("jpeo rvgh we iorgn", Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id"))
        );
        assertThrows(
            "no rerank type found",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );

        Map<String, Object> config2 = new HashMap<>(Map.of("key", Map.of(ByFieldRerankProcessor.TARGET_FIELD, "path.to.target_field")));
        assertThrows(
            "no rerank type found",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config2, pipelineContext)
        );
    }

    // Start of MLOpenSearchRerankProcessor Tests
    public void testCrossEncoder_whenCorrectParams_thenSuccessful() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>(List.of("text_representation"))))
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof MLOpenSearchRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testCrossEncoder_whenMessyConfig_thenSuccessful() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                "poafn aorr;anv",
                Map.of(";oawhls", "aowirhg "),
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id", "pqiohg rpowierhg", "pw;oith4pt3ih go")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>(List.of("text_representation"))))
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof MLOpenSearchRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testCrossEncoder_whenMessyContext_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(
                    Map.of(
                        DocumentContextSourceFetcher.NAME,
                        new ArrayList<>(List.of("text_representation")),
                        "pqiohg rpowierhg",
                        "pw;oith4pt3ih go"
                    )
                )
            )
        );
        assertThrows(
            String.format(Locale.ROOT, "unrecognized context field: %s", "pqiohg rpowierhg"),
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenEmptySubConfig_thenFail() {
        Map<String, Object> config = new HashMap<>(Map.of(RerankType.ML_OPENSEARCH.getLabel(), Map.of()));
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", RerankProcessorFactory.CONTEXT_CONFIG_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenNoContextField_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.ML_OPENSEARCH.getLabel(), new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")))
        );
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", RerankProcessorFactory.CONTEXT_CONFIG_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenNoModelId_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>(List.of("text_representation"))))
            )
        );
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", MLOpenSearchRerankProcessor.MODEL_ID_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenBadContextDocField_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, "text_representation"))
            )
        );
        assertThrows(
            String.format(Locale.ROOT, "%s must be a list of strings", DocumentContextSourceFetcher.NAME),
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenEmptyContextDocField_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>()))
            )
        );
        assertThrows(
            String.format(Locale.ROOT, "%s must be nonempty", DocumentContextSourceFetcher.NAME),
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testCrossEncoder_whenTooManyDocFields_thenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.ML_OPENSEARCH.getLabel(),
                new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, Collections.nCopies(75, "field")))
            )
        );
        assertThrows(
            String.format(
                Locale.ROOT,
                "%s must not contain more than %d fields. Configure by setting %s",
                DocumentContextSourceFetcher.NAME,
                RERANKER_MAX_DOC_FIELDS.get(clusterService.getSettings()),
                RERANKER_MAX_DOC_FIELDS.getKey()
            ),
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }
    // End of MLOpenSearchRerankProcessor Tests

    // Start of ByFieldRerankProcessor Tests
    public void testByFieldCreation_whenTargetFieldSpecifiedWithDefaultRemoveTargetFieldAndDefaultPreviousScore_thenSuccessful() {
        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.BY_FIELD.getLabel(), new HashMap<>(Map.of(ByFieldRerankProcessor.TARGET_FIELD, "path.to.target_field")))
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof ByFieldRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testByFieldCreation_whenTargetFieldSpecifiedWithManualRemoveTargetFieldAndPreviousKeptScore_thenSuccessful() {
        boolean removeTargetField = true;
        boolean keepPreviousScore = true;
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        "path.to.target_field",
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
                )
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof ByFieldRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testByFieldCreation_WithContext_thenSucceed() {
        // You can pass context but, it won't ever be used by ByFieldRerank
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(Map.of(ByFieldRerankProcessor.TARGET_FIELD, "path.to.target_field")),
                RerankProcessorFactory.CONTEXT_CONFIG_FIELD,
                new HashMap<>(Map.of(DocumentContextSourceFetcher.NAME, new ArrayList<>(List.of("text_representation"))))
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);

        assert (processor instanceof RerankProcessor);
        assert (processor instanceof ByFieldRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testByField_whenEmptySubConfig_thenFail() {
        Map<String, Object> config = new HashMap<>(Map.of(RerankType.BY_FIELD.getLabel(), new HashMap<>()));
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", ByFieldRerankProcessor.TARGET_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }
    // End of ByFieldRerankProcessor Tests

}
