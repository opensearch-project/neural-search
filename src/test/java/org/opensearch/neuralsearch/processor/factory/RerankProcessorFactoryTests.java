/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.CrossEncoderRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

@Log4j2
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
            Map.of("jpeo rvgh we iorgn", Map.of(CrossEncoderRerankProcessor.MODEL_ID_FIELD, "model-id"))
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
                RerankType.CROSS_ENCODER.getLabel(),
                new HashMap<>(
                    Map.of(
                        CrossEncoderRerankProcessor.MODEL_ID_FIELD,
                        "model-id",
                        CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD,
                        "text_representation"
                    )
                )
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof CrossEncoderRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testRerankProcessorFactory_CrossEncoder_MessyConfig_ThenHappy() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                "poafn aorr;anv",
                Map.of(";oawhls", "aowirhg "),
                RerankType.CROSS_ENCODER.getLabel(),
                new HashMap<>(
                    Map.of(
                        CrossEncoderRerankProcessor.MODEL_ID_FIELD,
                        "model-id",
                        CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD,
                        "text_representation",
                        "pqiohg rpowierhg",
                        "pw;oith4pt3ih go"
                    )
                )
            )
        );
        SearchResponseProcessor processor = factory.create(Map.of(), TAG, DESC, false, config, pipelineContext);
        assert (processor instanceof RerankProcessor);
        assert (processor instanceof CrossEncoderRerankProcessor);
        assert (processor.getType().equals(RerankProcessor.TYPE));
    }

    public void testRerankProcessorFactory_CrossEncoder_EmptySubConfig_ThenFail() {
        Map<String, Object> config = new HashMap<>(Map.of(RerankType.CROSS_ENCODER.getLabel(), Map.of()));
        assertThrows(
            CrossEncoderRerankProcessor.MODEL_ID_FIELD + " must be specified",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_CrossEncoder_NoContextField_ThenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.CROSS_ENCODER.getLabel(), new HashMap<>(Map.of(CrossEncoderRerankProcessor.MODEL_ID_FIELD, "model-id")))
        );
        assertThrows(
            CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD + " must be specified",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

    public void testRerankProcessorFactory_CrossEncoder_NoModelId_ThenFail() {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.CROSS_ENCODER.getLabel(),
                new HashMap<>(Map.of(CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD, "text_representation"))
            )
        );
        assertThrows(
            CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD + " must be specified",
            IllegalArgumentException.class,
            () -> factory.create(Map.of(), TAG, DESC, false, config, pipelineContext)
        );
    }

}
