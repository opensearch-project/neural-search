/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

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
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.factory.TextChunkingProcessorFactory;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.ALGORITHM_FIELD;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.IGNORE_MISSING;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;

public class TextChunkingProcessorTests extends OpenSearchTestCase {

    private TextChunkingProcessorFactory textChunkingProcessorFactory;

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String INPUT_FIELD = "body";
    private static final String INPUT_NESTED_FIELD_KEY = "nested";
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
        Metadata metadata = mock(Metadata.class);
        Environment environment = mock(Environment.class);
        Settings settings = Settings.builder()
            .put("index.mapping.depth.limit", 20)
            .put("index.analyze.max_token_count", 10000)
            .put("index.number_of_shards", 1)
            .build();
        when(environment.settings()).thenReturn(settings);
        ClusterState clusterState = mock(ClusterState.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(metadata.index(anyString())).thenReturn(null);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(clusterState);
        textChunkingProcessorFactory = new TextChunkingProcessorFactory(environment, clusterService, getAnalysisRegistry());

        NeuralSearchSettingsAccessor settingsAccessor = mock(NeuralSearchSettingsAccessor.class);
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        EventStatsManager.instance().initialize(settingsAccessor);
    }

    private Map<String, Object> createFixedTokenLengthParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FixedTokenLengthChunker.TOKEN_LIMIT_FIELD, 10);
        return parameters;
    }

    private List<Map<String, Object>> createSourceDataListNestedMap() {
        Map<String, Object> documents = new HashMap<>();
        documents.put(INPUT_FIELD, createSourceDataString());
        return List.of(documents, documents);
    }

    private Map<String, Object> createFixedTokenLengthParametersWithMaxChunkLimit(int maxChunkLimit) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(FixedTokenLengthChunker.TOKEN_LIMIT_FIELD, 10);
        parameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
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

    private Map<String, Object> createNestedFieldMapSingleField() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(INPUT_NESTED_FIELD_KEY, Map.of(INPUT_FIELD, OUTPUT_FIELD));
        return fieldMap;
    }

    private Map<String, Object> createNestedFieldMapMultipleField() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put(INPUT_NESTED_FIELD_KEY, Map.of(INPUT_FIELD + "_1", OUTPUT_FIELD + "_1", INPUT_FIELD + "_2", OUTPUT_FIELD + "_2"));
        return fieldMap;
    }

    @SneakyThrows
    private TextChunkingProcessor createDefaultAlgorithmInstance(Map<String, Object> fieldMap) {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextChunkingProcessor createFixedTokenLengthInstance(Map<String, Object> fieldMap) {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, createFixedTokenLengthParameters());
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextChunkingProcessor createFixedTokenLengthInstanceWithMaxChunkLimit(Map<String, Object> fieldMap, int maxChunkLimit) {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, createFixedTokenLengthParametersWithMaxChunkLimit(maxChunkLimit));
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextChunkingProcessor createDelimiterInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(DelimiterChunker.ALGORITHM_NAME, createDelimiterParameters());
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextChunkingProcessor createIgnoreMissingInstance() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        algorithmMap.put(DelimiterChunker.ALGORITHM_NAME, createDelimiterParameters());
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        config.put(IGNORE_MISSING, true);
        Map<String, Processor.Factory> registry = new HashMap<>();
        return textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testCreate_whenAlgorithmFieldMissing_thenFail() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        config.put(FIELD_MAP_FIELD, fieldMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        OpenSearchParseException openSearchParseException = assertThrows(
            OpenSearchParseException.class,
            () -> textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals(
            String.format(Locale.ROOT, "[%s] required property is missing", ALGORITHM_FIELD),
            openSearchParseException.getMessage()
        );
    }

    @SneakyThrows
    public void testCreate_whenMaxChunkLimitInvalidValue_thenFail() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, createFixedTokenLengthParametersWithMaxChunkLimit(-2));
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assert (illegalArgumentException.getMessage()
            .contains(String.format(Locale.ROOT, "Parameter [%s] must be positive", MAX_CHUNK_LIMIT_FIELD)));
    }

    @SneakyThrows
    public void testCreate_whenMaxChunkLimitDisabledValue_thenSucceed() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, createFixedTokenLengthParametersWithMaxChunkLimit(-1));
        config.put(FIELD_MAP_FIELD, fieldMap);
        config.put(ALGORITHM_FIELD, algorithmMap);
        textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testCreate_whenAlgorithmMapMultipleAlgorithms_thenFail() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(TextChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, createFixedTokenLengthParameters());
        algorithmMap.put(DelimiterChunker.ALGORITHM_NAME, createDelimiterParameters());
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals(
            String.format(Locale.ROOT, "Unable to create %s processor as [%s] contains multiple algorithms", TYPE, ALGORITHM_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    public void testCreate_wheAlgorithmMapInvalidAlgorithmName_thenFail() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        String invalid_algorithm_type = "invalid algorithm";
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(TextChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        algorithmMap.put(invalid_algorithm_type, createFixedTokenLengthParameters());
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assert (illegalArgumentException.getMessage()
            .contains(String.format(Locale.ROOT, "Chunking algorithm [%s] is not supported.", invalid_algorithm_type)));
    }

    public void testCreate_whenAlgorithmMapInvalidAlgorithmType_thenFail() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> algorithmMap = new HashMap<>();
        fieldMap.put(INPUT_FIELD, OUTPUT_FIELD);
        config.put(TextChunkingProcessor.FIELD_MAP_FIELD, fieldMap);
        algorithmMap.put(FixedTokenLengthChunker.ALGORITHM_NAME, 1);
        config.put(ALGORITHM_FIELD, algorithmMap);
        Map<String, Processor.Factory> registry = new HashMap<>();
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> textChunkingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals(
            String.format(
                Locale.ROOT,
                "Unable to create %s processor as parameters for [%s] algorithm must be an object",
                TYPE,
                FixedTokenLengthChunker.ALGORITHM_NAME
            ),
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testGetType() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        String type = processor.getType();
        assertEquals(TYPE, type);
    }

    private String createSourceDataString() {
        return "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
    }

    private List<String> createSourceDataListStrings() {
        List<String> documents = new ArrayList<>();
        documents.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.add("");
        documents.add(
            "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        return documents;
    }

    private List<Object> createSourceDataListWithInvalidType() {
        List<Object> documents = new ArrayList<>();
        documents.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.add(1);
        return documents;
    }

    private List<Object> createSourceDataListWithHybridType() {
        List<Object> documents = new ArrayList<>();
        documents.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.add(ImmutableMap.of());
        return documents;
    }

    private List<Object> createSourceDataListWithNull() {
        List<Object> documents = new ArrayList<>();
        documents.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        documents.add(null);
        return documents;
    }

    private Map<String, Object> createSourceDataNestedMapSingleField() {
        Map<String, Object> documents = new HashMap<>();
        documents.put(INPUT_FIELD, createSourceDataString());
        return documents;
    }

    private Map<String, Object> createSourceDataNestedMapMultipleField() {
        Map<String, Object> documents = new HashMap<>();
        documents.put(INPUT_FIELD + "_1", createSourceDataString());
        documents.put(INPUT_FIELD + "_2", createSourceDataString());
        return documents;
    }

    private Map<String, Object> createSourceDataInvalidNestedMap() {
        Map<String, Object> documents = new HashMap<>();
        documents.put(INPUT_FIELD, Map.of(INPUT_NESTED_FIELD_KEY, 1));
        return documents;
    }

    private Map<String, Object> createMaxDepthLimitExceedMap(int maxDepth) {
        if (maxDepth > 21) {
            return Map.of(INPUT_FIELD, "mapped");
        }
        Map<String, Object> resultMap = new HashMap<>();
        Map<String, Object> innerMap = createMaxDepthLimitExceedMap(maxDepth + 1);
        resultMap.put(INPUT_FIELD, innerMap);
        return resultMap;
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
    public void testExecute_withFixedTokenLength_andSourceDataStringWithMaxChunkLimit_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), 5);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataStringWithMaxChunkLimitTwice_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), 5);
        for (int i = 0; i < 2; i++) {
            IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
            IngestDocument document = processor.execute(ingestDocument);
            assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
            Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
            assert (passages instanceof List<?>);
            List<String> expectedPassages = new ArrayList<>();
            expectedPassages.add("This is an example document to be chunked. The document ");
            expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
            expectedPassages.add("standard tokenizer in OpenSearch.");
            assertEquals(expectedPassages, passages);
        }
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataStringWithMaxChunkLimitDisabled_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), -1);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataStringExceedMaxChunkLimit_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 1;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = List.of(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListExceedMaxChunkLimitFive_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 5;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        expectedPassages.add("This is the second document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListExceedMaxChunkLimitFour_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 4;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        expectedPassages.add(
            "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListExceedMaxChunkLimitThree_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 3;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        expectedPassages.add(
            "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListExceedMaxChunkLimitTwo_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 2;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        expectedPassages.add(
            "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListExceedMaxChunkLimitOne_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 1;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is the first document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        expectedPassages.add(
            "This is the second document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListDisabledMaxChunkLimit_thenSuccessful() {
        int maxChunkLimit = -1;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(createStringFieldMap(), maxChunkLimit);
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        expectedPassages.add("This is the second document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testCreate_withDefaultAlgorithm_andSourceDataString_thenSucceed() {
        TextChunkingProcessor processor = createDefaultAlgorithmInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataString_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataString());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataInvalidType_thenFail() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(INPUT_FIELD, 1);
        sourceAndMetadata.put(IndexFieldMapper.NAME, INDEX_NAME);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals(
            String.format(Locale.ROOT, "map type field [%s] is neither string nor nested type, cannot process it", INPUT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListStrings_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListStrings());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(OUTPUT_FIELD);
        Object passages = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (passages instanceof List<?>);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is the first document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        expectedPassages.add("This is the second document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListWithInvalidType_thenFail() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListWithInvalidType());
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals(
            String.format(Locale.ROOT, "list type field [%s] has non string value, cannot process it", INPUT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListWithNull_thenFail() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(createSourceDataListWithNull());
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals(
            String.format(Locale.ROOT, "list type field [%s] has null, cannot process it", INPUT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapSingleField_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createNestedFieldMapSingleField());
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapSingleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD);
        Object passages = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD);
        assert (passages instanceof List);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createNestedFieldMapMultipleField());
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by ",
            "standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages1);
        assertEquals(expectedPassages, passages2);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public
        void
        testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_exceedMaxChunkLimitFive_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 5;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(
            createNestedFieldMapMultipleField(),
            maxChunkLimit
        );
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages1 = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by ",
            "standard tokenizer in OpenSearch."
        );
        List<String> expectedPassages2 = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        Set<List<String>> passages = Set.of((List<String>) passages1, (List<String>) passages2);
        Set<List<String>> expectedPassages = Set.of(expectedPassages1, expectedPassages2);
        assertEquals(passages, expectedPassages);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public
        void
        testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_exceedMaxChunkLimitFour_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 4;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(
            createNestedFieldMapMultipleField(),
            maxChunkLimit
        );
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages1 = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by ",
            "standard tokenizer in OpenSearch."
        );
        List<String> expectedPassages2 = List.of(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        Set<List<String>> passages = Set.of((List<String>) passages1, (List<String>) passages2);
        Set<List<String>> expectedPassages = Set.of(expectedPassages1, expectedPassages2);
        assertEquals(passages, expectedPassages);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public
        void
        testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_exceedMaxChunkLimitThree_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 3;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(
            createNestedFieldMapMultipleField(),
            maxChunkLimit
        );
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages1 = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        List<String> expectedPassages2 = List.of(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        Set<List<String>> passages = Set.of((List<String>) passages1, (List<String>) passages2);
        Set<List<String>> expectedPassages = Set.of(expectedPassages1, expectedPassages2);
        assertEquals(passages, expectedPassages);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_exceedMaxChunkLimitTwo_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 2;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(
            createNestedFieldMapMultipleField(),
            maxChunkLimit
        );
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages = List.of(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(passages1, expectedPassages);
        assertEquals(passages2, expectedPassages);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapMultipleField_exceedMaxChunkLimitOne_thenLastPassageGetConcatenated() {
        int maxChunkLimit = 1;
        TextChunkingProcessor processor = createFixedTokenLengthInstanceWithMaxChunkLimit(
            createNestedFieldMapMultipleField(),
            maxChunkLimit
        );
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataNestedMapMultipleField());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);
        assert (nestedResult instanceof Map<?, ?>);
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_1");
        assert ((Map<String, Object>) nestedResult).containsKey(OUTPUT_FIELD + "_2");
        Object passages1 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_1");
        Object passages2 = ((Map<String, Object>) nestedResult).get(OUTPUT_FIELD + "_2");
        assert (passages1 instanceof List);
        assert (passages2 instanceof List);

        List<String> expectedPassages = List.of(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(passages1, expectedPassages);
        assertEquals(passages2, expectedPassages);
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andMaxDepthLimitExceedFieldMap_thenFail() {
        Map<String, Object> map = createMaxDepthLimitExceedMap(0);
        Map<String, Object> config = new HashMap<>();
        config.put(INPUT_NESTED_FIELD_KEY, map.get("body"));
        TextChunkingProcessor processor = createFixedTokenLengthInstance(config);
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(map);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals("map type field [body] reaches max depth limit, cannot process it", illegalArgumentException.getMessage());
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapSingleField_thenFail() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createNestedFieldMapSingleField());
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataInvalidNestedMap());
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals(
            "[body] configuration doesn't match actual value type, configuration type is: java.lang.String, actual value type is: java.util.ImmutableCollections$Map1",
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testExecute_withFixedTokenLength_andFieldMapNestedMapSingleField_sourceDataList_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createNestedFieldMapSingleField());
        IngestDocument ingestDocument = createIngestDocumentWithNestedSourceData(createSourceDataListNestedMap());
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_NESTED_FIELD_KEY);
        Object nestedResult = document.getSourceAndMetadata().get(INPUT_NESTED_FIELD_KEY);

        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assert (nestedResult instanceof List);
        assertEquals(((List<?>) nestedResult).size(), 2);
        for (Object result : (List<Object>) nestedResult) {
            assert (result instanceof Map);
            assert ((Map<String, Object>) result).containsKey(OUTPUT_FIELD);
            Object passages = ((Map<?, ?>) result).get(OUTPUT_FIELD);
            assert (passages instanceof List);
            assertEquals(expectedPassages, passages);
        }
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataListWithHybridType_thenFail() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        List<Object> sourceDataList = createSourceDataListWithHybridType();
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(sourceDataList);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );
        assertEquals(
            "[body] configuration doesn't match actual value type, configuration type is: java.lang.String, actual value type is: com.google.common.collect.RegularImmutableMap",
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testExecute_withFixedTokenLength_andSourceDataNull_thenSucceed() {
        TextChunkingProcessor processor = createFixedTokenLengthInstance(createStringFieldMap());
        IngestDocument ingestDocument = createIngestDocumentWithSourceData(null);
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey(INPUT_FIELD);
        Object listResult = document.getSourceAndMetadata().get(OUTPUT_FIELD);
        assert (listResult instanceof List);
        assertEquals(((List<?>) listResult).size(), 0);
    }

    @SneakyThrows
    public void testExecute_withDelimiter_andSourceDataString_thenSucceed() {
        TextChunkingProcessor processor = createDelimiterInstance();
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

    @SneakyThrows
    public void testExecute_withIgnoreMissing_thenSucceed() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("text_field", "");
        sourceAndMetadata.put(IndexFieldMapper.NAME, INDEX_NAME);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        TextChunkingProcessor processor = createIgnoreMissingInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assertFalse(document.getSourceAndMetadata().containsKey(OUTPUT_FIELD));
    }
}
