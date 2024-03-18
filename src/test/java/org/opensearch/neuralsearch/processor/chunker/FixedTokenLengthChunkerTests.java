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
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ALGORITHM_NAME;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKEN_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.OVERLAP_RATE_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKENIZER_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD;

public class FixedTokenLengthChunkerTests extends OpenSearchTestCase {

    private FixedTokenLengthChunker fixedTokenLengthChunker;

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
        fixedTokenLengthChunker.parseParameters(Map.of());
    }

    public void testParseParameters_whenIllegalTokenLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, "invalid token limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.parseParameters(parameters)
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
            () -> fixedTokenLengthChunker.chunk(content, parameters)
        );
        assert (illegalStateException.getMessage()
            .contains(String.format(Locale.ROOT, "analyzer %s throws exception", lowercaseTokenizer)));
    }

    public void testChunk_withEmptyInput_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content = "";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        assert (passages.isEmpty());
    }

    public void testChunk_withTokenLimit10_thenSucceed() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
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
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
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
        List<String> passages = fixedTokenLengthChunker.chunk(content, Map.of());
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("to be chunked. The document contains a single paragraph, two ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedMaxChunkLimit_thenFail() {
        int maxChunkLimit = 2;
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        parameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.chunk(content, runtimeParameters)
        );
        assert (illegalArgumentException.getMessage()
            .contains(
                String.format(
                    Locale.ROOT,
                    "The number of chunks produced by %s processor has exceeded the allowed maximum of [%s].",
                    TYPE,
                    maxChunkLimit
                )
            ));
    }

    public void testChunk_whenWithinMaxChunkLimit_thenSucceed() {
        int maxChunkLimit = 3;
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        parameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_whenExceedRuntimeMaxChunkLimit_thenFail() {
        int maxChunkLimit = 3, runtimeMaxChunkLimit = 2;
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        parameters.put(MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.chunk(content, runtimeParameters)
        );
        assert (illegalArgumentException.getMessage()
            .contains(
                String.format(
                    Locale.ROOT,
                    "The number of chunks produced by %s processor has exceeded the allowed maximum of [%s].",
                    TYPE,
                    maxChunkLimit
                )
            ));
    }
}
