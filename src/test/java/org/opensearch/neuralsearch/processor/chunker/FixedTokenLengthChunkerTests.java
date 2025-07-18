/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ALGORITHM_NAME;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKEN_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.OVERLAP_RATE_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKENIZER_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD;

public class FixedTokenLengthChunkerTests extends OpenSearchTestCase {

    private FixedTokenLengthChunker fixedTokenLengthChunker;

    private final Map<String, Object> runtimeParameters = Map.of(
        MAX_CHUNK_LIMIT_FIELD,
        100,
        CHUNK_STRING_COUNT_FIELD,
        1,
        MAX_TOKEN_COUNT_FIELD,
        10000
    );

    @Before
    public void setup() {
        fixedTokenLengthChunker = createFixedTokenLengthChunker(Map.of());
    }

    @SneakyThrows
    public FixedTokenLengthChunker createFixedTokenLengthChunker(Map<String, Object> parameters) {
        Map<String, Object> nonRuntimeParameters = new HashMap<>(parameters);
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
        AnalysisRegistry analysisRegistry = new AnalysisModule(environment, singletonList(plugin)).getAnalysisRegistry();
        nonRuntimeParameters.put(ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        return new FixedTokenLengthChunker(nonRuntimeParameters);
    }

    public void testParseParameters_whenNoParams_thenSuccessful() {
        fixedTokenLengthChunker.parse(Map.of());
    }

    public void testParseParameters_whenIllegalTokenLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, "invalid token limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", TOKEN_LIMIT_FIELD, Integer.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalTokenLimitValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", TOKEN_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalOverlapRateType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, "invalid overlap rate");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", OVERLAP_RATE_FIELD, Double.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenTooLargeOverlapRate_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, 0.6);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s", OVERLAP_RATE_FIELD, 0.0, 0.5),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenTooSmallOverlapRateValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s", OVERLAP_RATE_FIELD, 0.0, 0.5),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenIllegalTokenizerType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKENIZER_FIELD, 111);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", TOKENIZER_FIELD, String.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testParseParameters_whenUnsupportedTokenizer_thenFail() {
        String ngramTokenizer = "ngram";
        Map<String, Object> parameters = Map.of(TOKENIZER_FIELD, ngramTokenizer);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parse(parameters)
        );
        assert (illegalArgumentException.getMessage()
            .contains(String.format(Locale.ROOT, "Tokenizer [%s] is not supported for [%s] algorithm.", ngramTokenizer, ALGORITHM_NAME)));
    }

    public void testChunk_whenTokenizationException_thenFail() {
        // lowercase tokenizer is not supported in unit tests
        String lowercaseTokenizer = "lowercase";
        Map<String, Object> parameters = Map.of(TOKENIZER_FIELD, lowercaseTokenizer);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        IllegalStateException illegalStateException = assertThrows(
            IllegalStateException.class,
            () -> fixedTokenLengthChunker.chunk(content, runtimeParameters)
        );
        assert (illegalStateException.getMessage()
            .contains(String.format(Locale.ROOT, "analyzer %s throws exception", lowercaseTokenizer)));
    }

    public void testChunk_withEmptyInput_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        String content = "";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        assert (passages.isEmpty());
    }

    public void testChunk_withTokenLimit10_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withTokenLimit20_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 20);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>(this.runtimeParameters);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by "
        );
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRateHalf_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(OVERLAP_RATE_FIELD, 0.5);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("to be chunked. The document contains a single paragraph, two ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedMaxChunkLimit_thenLastPassageGetConcatenated() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        int runtimeMaxChunkLimit = 2;
        Map<String, Object> runtimeParameters = new HashMap<>(this.runtimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenWithinMaxChunkLimit_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        int runtimeMaxChunkLimit = 3;
        Map<String, Object> runtimeParameters = new HashMap<>(this.runtimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedRuntimeMaxChunkLimit_thenLastPassageGetConcatenated() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        int runtimeMaxChunkLimit = 2;
        Map<String, Object> runtimeParameters = new HashMap<>(this.runtimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedRuntimeMaxChunkLimit_withOneStringTobeChunked_thenLastPassageGetConcatenated() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        int runtimeMaxChunkLimit = 2, chunkStringCount = 1;
        Map<String, Object> runtimeParameters = new HashMap<>(this.runtimeParameters);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, chunkStringCount);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = List.of(
            "This is an example document to be chunked. The document ",
            "contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch."
        );
        assertEquals(expectedPassages, passages);
    }

    public void testValidateParameters_whenInvalidTokenizer_thenThrowException() {
        final Validator validator = new FixedTokenLengthChunker();
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> validator.validate(Map.of(TOKENIZER_FIELD, "invalid"))
        );

        assertTrue(exception.getMessage().contains("Tokenizer [invalid] is not supported for [fixed_token_length] algorithm."));
    }
}
