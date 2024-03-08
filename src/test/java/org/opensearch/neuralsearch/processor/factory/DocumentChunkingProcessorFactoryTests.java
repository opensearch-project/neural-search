/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.DocumentChunkingProcessor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.ALGORITHM_FIELD;

public class DocumentChunkingProcessorFactoryTests extends OpenSearchTestCase {

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final Map<String, Object> algorithmMap = Map.of(ChunkerFactory.FIXED_LENGTH_ALGORITHM, new HashMap<>());

    private DocumentChunkingProcessorFactory documentChunkingProcessorFactory;

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
        this.documentChunkingProcessorFactory = new DocumentChunkingProcessorFactory(
            environment,
            clusterService,
            indicesService,
            getAnalysisRegistry()
        );
    }

    @SneakyThrows
    public void testDocumentChunkingProcessorFactory_whenAllParamsPassed_thenSuccessful() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ALGORITHM_FIELD, algorithmMap);
        config.put(FIELD_MAP_FIELD, new HashMap<>());
        DocumentChunkingProcessor documentChunkingProcessor = documentChunkingProcessorFactory.create(
            processorFactories,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        assertNotNull(documentChunkingProcessor);
        assertEquals(TYPE, documentChunkingProcessor.getType());
    }

    @SneakyThrows
    public void testDocumentChunkingProcessorFactory_whenOnlyFieldMap_thenFail() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(FIELD_MAP_FIELD, new HashMap<>());
        Exception exception = assertThrows(
            Exception.class,
            () -> documentChunkingProcessorFactory.create(processorFactories, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + ALGORITHM_FIELD + "] required property is missing", exception.getMessage());
    }

    @SneakyThrows
    public void testDocumentChunkingProcessorFactory_whenOnlyAlgorithm_thenFail() {
        final Map<String, Processor.Factory> processorFactories = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ALGORITHM_FIELD, algorithmMap);
        Exception exception = assertThrows(
            Exception.class,
            () -> documentChunkingProcessorFactory.create(processorFactories, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + FIELD_MAP_FIELD + "] required property is missing", exception.getMessage());
    }
}
