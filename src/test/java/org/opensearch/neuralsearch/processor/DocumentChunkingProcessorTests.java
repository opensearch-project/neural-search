/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableMap;
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

public class DocumentChunkingProcessorTests extends OpenSearchTestCase {

    private DocumentChunkingProcessor.Factory factory;

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String INPUT_FIELD = "body";
    private static final String OUTPUT_FIELD = "body_chunk";
    private static final String INDEX_NAME = "_index";

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
        parameters.put(FixedTokenLengthChunker.TOKEN_LIMIT_FIELD, 10);
        return parameters;
    }

    private Map<String, Object> createDelimiterParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(DelimiterChunker.DELIMITER_FIELD, ".");
        return parameters;
    }

    @SneakyThrows
    private DocumentChunkingProcessor createFixedTokenLengthInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldParameters = new HashMap<>();
        Map<String, Object> chunkerParameters = new HashMap<>();
        chunkerParameters.put(ChunkerFactory.FIXED_LENGTH_ALGORITHM, createFixedTokenLengthParameters());
        chunkerParameters.put(DocumentChunkingProcessor.OUTPUT_FIELD, OUTPUT_FIELD);
        fieldParameters.put(INPUT_FIELD, chunkerParameters);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldParameters);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private DocumentChunkingProcessor createDelimiterInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldParameters = new HashMap<>();
        Map<String, Object> chunkerParameters = new HashMap<>();
        chunkerParameters.put(ChunkerFactory.DELIMITER_ALGORITHM, createDelimiterParameters());
        chunkerParameters.put(DocumentChunkingProcessor.OUTPUT_FIELD, OUTPUT_FIELD);
        fieldParameters.put(INPUT_FIELD, chunkerParameters);
        config.put(DocumentChunkingProcessor.FIELD_MAP_FIELD, fieldParameters);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return factory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
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
        String documentString = createSourceDataString();
        List<String> documentList = createSourceDataList();
        Map<String, String> documentMap = createSourceDataMap();
        Map<String, Object> documents = new HashMap<>();
        documents.put("String", documentString);
        documents.put("List", documentList);
        documents.put("Map", documentMap);
        return documents;
    }

    private IngestDocument createIngestDocumentWithSourceData(Object sourceData) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(INPUT_FIELD, sourceData);
        sourceAndMetadata.put(IndexFieldMapper.NAME, INDEX_NAME);
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataString_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
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
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
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
    public void testExecute_withFixedTokenLength_andSourceDataMap_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataMap());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof Map<?, ?>);

        List<String> expectedPassages1 = new ArrayList<>();
        List<String> expectedPassages2 = new ArrayList<>();

        expectedPassages1.add("This is the third document to be chunked The document");
        expectedPassages1.add("The document contains a single paragraph two sentences and 24");
        expectedPassages1.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages2.add("This is the fourth document to be chunked The document");
        expectedPassages2.add("The document contains a single paragraph two sentences and 24");
        expectedPassages2.add("and 24 tokens by standard tokenizer in OpenSearch");

        Map<String, Object> expectedPassages = ImmutableMap.of("third", expectedPassages1, "fourth", expectedPassages2);

        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataNestedMap_successful() {
        DocumentChunkingProcessor processor = createFixedTokenLengthInstance();
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataNestedMap());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof Map<?, ?>);

        Map<String, Object> expectedPassages = new HashMap<>();
        List<String> expectedPassages1 = new ArrayList<>();
        List<String> expectedPassages2 = new ArrayList<>();
        List<String> expectedPassages3 = new ArrayList<>();
        List<String> expectedPassages4 = new ArrayList<>();

        expectedPassages1.add("This is an example document to be chunked The document");
        expectedPassages1.add("The document contains a single paragraph two sentences and 24");
        expectedPassages1.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages2.add("This is the first document to be chunked The document");
        expectedPassages2.add("The document contains a single paragraph two sentences and 24");
        expectedPassages2.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages2.add("This is the second document to be chunked The document");
        expectedPassages2.add("The document contains a single paragraph two sentences and 24");
        expectedPassages2.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages3.add("This is the third document to be chunked The document");
        expectedPassages3.add("The document contains a single paragraph two sentences and 24");
        expectedPassages3.add("and 24 tokens by standard tokenizer in OpenSearch");
        expectedPassages4.add("This is the fourth document to be chunked The document");
        expectedPassages4.add("The document contains a single paragraph two sentences and 24");
        expectedPassages4.add("and 24 tokens by standard tokenizer in OpenSearch");

        expectedPassages.put("String", expectedPassages1);
        expectedPassages.put("List", expectedPassages2);
        expectedPassages.put("Map", ImmutableMap.of("third", expectedPassages3, "fourth", expectedPassages4));

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
