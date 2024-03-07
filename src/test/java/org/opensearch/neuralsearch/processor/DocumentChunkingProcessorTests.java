/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.OpenSearchParseException;
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
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
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
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.ALGORITHM_FIELD;

public class DocumentChunkingProcessorTests extends OpenSearchTestCase {

    private DocumentChunkingProcessor.Factory factory;

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String INPUT_FIELD = "body";

    private static final String INPUT_NESTED_FIELD_KEY = "nested";
    private static final String OUTPUT_FIELD = "body_chunk";
    private static final String INDEX_NAME = "_index";

    @Mock
    private Environment environment;

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
        Metadata metadata = mock(Metadata.class);
        Environment environment = mock(Environment.class);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(environment.settings()).thenReturn(settings);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        IndicesService indicesService = mock(IndicesService.class);
        when(metadata.index(anyString())).thenReturn(null);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(clusterState);
        factory = new DocumentChunkingProcessor.Factory(environment, clusterService, indicesService, getAnalysisRegistry());
    }

    private Map<String, Object> createFixedTokenLengthParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FixedTokenLengthChunker.TOKEN_LIMIT_FIELD, 10);
        return parameters;
    }

    private Map<String, Object> createDelimiterParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(DelimiterChunker.DELIMITER_FIELD, ".");
        return parameters;
    }

    private Map<String, Object> createStringFieldMap() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        return fieldMap;
    }

    private Map<String, Object> createNestedFieldMap() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(INPUT_NESTED_FIELD_KEY, Map.of(INPUT_FIELD, OUTPUT_FIELD));
        return fieldMap;
    }

    @SneakyThrows
    private DocumentChunkingProcessor createFixedTokenLengthInstance(Map<String, Object> fieldMap) {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(ChunkerFactory.FIXED_LENGTH_ALGORITHM, createFixedTokenLengthParameters());
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private DocumentChunkingProcessor createDelimiterInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(ChunkerFactory.DELIMITER_ALGORITHM, createDelimiterParameters());
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testCreate_whenFieldMapEmpty_failure() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> emptyFieldMap = new HashMap<>();
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, emptyFieldMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        OpenSearchParseException openSearchParseException = assertThrows(
                OpenSearchParseException.class,
                () -> factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + ALGORITHM_FIELD + "] required property is missing", openSearchParseException.getMessage());
    }

    public void testCreate_whenFieldMapWithEmptyParameter_failure() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("key", null);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        OpenSearchParseException openSearchParseException = assertThrows(
                OpenSearchParseException.class,
                () -> factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + ALGORITHM_FIELD + "] required property is missing", openSearchParseException.getMessage());
    }

    public void testCreate_whenFieldMapWithIllegalParameterType_failure() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("key", "value");
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        OpenSearchParseException openSearchParseException = assertThrows(
                OpenSearchParseException.class,
                () -> factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("[" + ALGORITHM_FIELD + "] required property is missing", openSearchParseException.getMessage());
    }

    public void testCreate_whenFieldMapWithNoAlgorithm_failure() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals(
                "Unable to create the processor as [" + ALGORITHM_FIELD + "] must contain and only contain 1 algorithm",
                illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testGetType() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        String type = processor.getType();
        assertEquals(DocumentChunkingProcessor.TYPE, type);
    }

    private String createSourceDataString() {
        return "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
    }

    private List<String> createSourceDataList() {
        List<String> documents = new ArrayList<>();
        documents.add(
                "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.add(
                "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        return documents;
    }

    private Map<String, String> createSourceDataMap() {
        Map<String, String> documents = new HashMap<>();
        documents.put(
                "third",
                "This is the third document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.put(
                "fourth",
                "This is the fourth document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        return documents;
    }

    private Map<String, Object> createSourceDataNestedMap() {
        Map<String, Object> documents = new HashMap<>();
        documents.put(INPUT_FIELD, createSourceDataString());
        return documents;
    }

    private IngestDocument createIngestDocumentWithNestedSourceData(Object sourceData) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(INPUT_NESTED_FIELD_KEY, sourceData);
        sourceAndMetadata.put(IndexFieldMapper.NAME, INDEX_NAME);
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    private IngestDocument createIngestDocumentWithSourceData(Object sourceData) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(INPUT_FIELD, sourceData);
        sourceAndMetadata.put(IndexFieldMapper.NAME, INDEX_NAME);
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataString_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked The document");
        expectedPassages.add("The document contains a single paragraph two sentences and 24");
        expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataList_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataList());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked The document");
        expectedPassages.add("The document contains a single paragraph two sentences and 24");
        expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages.add("This is the second document to be chunked The document");
        expectedPassages.add("The document contains a single paragraph two sentences and 24");
        expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andFieldMapNestedMap_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance(createNestedFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMap());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD);
        Object passages = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD);
        assert (passages instanceof List);

        List<String> expectedPassages = new ArrayList<>();

        expectedPassages.add("This is an example document to be chunked The document");
        expectedPassages.add("The document contains a single paragraph two sentences and 24");
        expectedPassages.add("and 24 tokens by standard tokenizer in OpenSearch");

        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withDelimiter_andSourceDataString_successful() {
        DocumentChunkingProcessor processor = createDelimiterInstance();
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked.");
        expectedPassages.add(" The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }
}
