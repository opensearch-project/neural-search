/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class DocumentChunkingProcessorTests extends OpenSearchTestCase {

    private DocumentChunkingProcessor.Factory factory;

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

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
        Settings settings = Settings.builder().build();
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        IndicesService indicesService = mock(IndicesService.class);
        when(metadata.index(anyString())).thenReturn(null);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(clusterState);
        factory = new DocumentChunkingProcessor.Factory(settings, clusterService, indicesService, getAnalysisRegistry());
    }

    @SneakyThrows
    public void testGetType() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
        String type = processor.getType();
        assertEquals(DocumentChunkingProcessor.TYPE, type);
    }

    private Map<String, Object> createFixedTokenLengthParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FixedTokenLengthChunker.TOKEN_LIMIT, 10);
        return parameters;
    }

    @SneakyThrows
    private DocumentChunkingProcessor createFixedTokenLengthInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldParameters = new HashMap<>();
        Map<String, Object> chunkerParameters = new HashMap<>();
        chunkerParameters.put(ChunkerFactory.FIXED_LENGTH_ALGORITHM, createFixedTokenLengthParameters());
        chunkerParameters.put(DocumentChunkingProcessor.OUTPUT_FIELD, "body_chunk");
        fieldParameters.put("body", chunkerParameters);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldParameters);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    private IngestDocument createIngestDocument() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(
            "body",
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        sourceAndMetadata.put(IndexFieldMapper.NAME, "_index");
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
        IngestDocument ingestDocument = createIngestDocument();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("body_chunk");
        Object passages = document.getSourceAndMetadata().get("body_chunk");
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked The document");
        expectedPassages.add("The document contains a single paragraph two sentences and 24");
        expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }
}
