/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import java.util.HashMap;
import java.util.Map;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.ALGORITHM_FIELD;

public class TextChunkingProcessorFactoryTests extends OpenSearchTestCase {

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final Map<String, Object> algorithmMap = Map.of(ChunkerFactory.FIXED_TOKEN_LENGTH_ALGORITHM, new HashMap<>());

    private TextChunkingProcessorFactory textChunkingProcessorFactory;

    @SneakyThrows
    private AnalysisRegistry getAnalysisRegistry() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisPlugin plugin = new AnalysisPlugin() {

            @Override
            public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
                return singletonMap(
                    "keyword",
                    (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                        name,
                        () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                    )
                );
            }
        };
        return new AnalysisModule(environment, singletonList(plugin)).getAnalysisRegistry();
    }

    @Before
    public void setup() {
        Environment environment = mock(Environment.class);
        ClusterService clusterService = mock(ClusterService.class);
        IndicesService indicesService = mock(IndicesService.class);
        this.textChunkingProcessorFactory = new TextChunkingProcessorFactory(
            environment,
            clusterService,
            indicesService,
            getAnalysisRegistry()
        );
    }

    @SneakyThrows
    public void testTextChunkingProcessorFactory_whenAllParamsPassed_thenSuccessful() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ALGORITHM_FIELD, algorithmMap);
        config.put(FIELD_MAP_FIELD, new HashMap<>());
        TextChunkingProcessor textChunkingProcessor = textChunkingProcessorFactory.create(
            processorFactories,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        assertNotNull(textChunkingProcessor);
        assertEquals(TYPE, textChunkingProcessor.getType());
    }

    @SneakyThrows
    public void testTextChunkingProcessorFactory_whenOnlyFieldMap_thenFail() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(FIELD_MAP_FIELD, new HashMap<>());
        Exception exception = assertThrows(
            Exception.class,
            () -> textChunkingProcessorFactory.create(processorFactories, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + ALGORITHM_FIELD + "] required property is missing", exception.getMessage());
    }

    @SneakyThrows
    public void testTextChunkingProcessorFactory_whenOnlyAlgorithm_thenFail() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ALGORITHM_FIELD, algorithmMap);
        Exception exception = assertThrows(
            Exception.class,
            () -> textChunkingProcessorFactory.create(processorFactories, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + FIELD_MAP_FIELD + "] required property is missing", exception.getMessage());
    }
}
