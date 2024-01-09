/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.OpenSearchParseException;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.MLOpenSearchRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class RerankProcessorFactoryTests extends OpenSearchTestCase {

    final String TAG = "default-tag";
    final String DESC = "processor description";

    RerankProcessorFactory factory;

    @Mock
    MLCommonsClientAccessor clientAccessor;

    @Mock
    PipelineContext pipelineContext;

    @Before
    public void setup() {
        pipelineContext = mock(PipelineContext.class);
        clientAccessor = mock(MLCommonsClientAccessor.class);
        factory = new RerankProcessorFactory(clientAccessor);
    }

    public void testRerankProcessorFactory_EmptyConfig_ThenFail() {
        Map<String, Object> config = new HashMap<>(Map.of());
        assertThrows(
            "no rerank type found",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_NonExistentType_ThenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of("jpeo rvgh we iorgn", Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id"))
        );
        assertThrows(
            "no rerank type found",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_CrossEncoder_HappyPath() {
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

    public void testRerankProcessorFactory_CrossEncoder_MessyConfig_ThenHappy() {
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

    public void testRerankProcessorFactory_CrossEncoder_MessyContext_ThenFail() {
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

    public void testRerankProcessorFactory_CrossEncoder_EmptySubConfig_ThenFail() {
        Map<String, Object> config = new HashMap<>(Map.of(RerankType.ML_OPENSEARCH.getLabel(), Map.of()));
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", RerankProcessorFactory.CONTEXT_CONFIG_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_CrossEncoder_NoContextField_ThenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.ML_OPENSEARCH.getLabel(), new HashMap<>(Map.of(MLOpenSearchRerankProcessor.MODEL_ID_FIELD, "model-id")))
        );
        assertThrows(
            String.format(Locale.ROOT, "[%s] required property is missing", RerankProcessorFactory.CONTEXT_CONFIG_FIELD),
            OpenSearchParseException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_CrossEncoder_NoModelId_ThenFail() {
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

    public void testRerankProcessorFactory_CrossEncoder_BadContextDocField_ThenFail() {
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

    public void testRerankProcessorFactory_CrossEncoder_EmptyContextDocField_ThenFail() {
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

}
